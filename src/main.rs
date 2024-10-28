use mirmod_rs::config::MirandaConfig;
use mirmod_rs::orm::bigdecimal::ToPrimitive;
use mirmod_rs::orm::ORMObject;
use std::io::{BufRead, Error};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{CpuExt, NetworkExt, System, SystemExt};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Copy, Clone)]
enum LogLevel {
    Default = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
    Debug = 4,
}

fn match_line_to_tag(line: &str) -> LogLevel {
    if line.starts_with("[INFO]") || line.starts_with("INFO:") {
        return LogLevel::Info;
    } else if line.starts_with("[WARN]") || line.starts_with("WARN:") {
        return LogLevel::Warning;
    } else if line.starts_with("[ERROR]") || line.starts_with("ERROR:") {
        return LogLevel::Error;
    } else if line.starts_with("[DEBUG]") || line.starts_with("DEBUG:") || line.starts_with("|=>") {
        return LogLevel::Debug;
    }
    LogLevel::Default
}

#[derive(Clone)]
struct LogLine {
    timestamp: chrono::DateTime<chrono::Utc>,
    line: String,
    level: LogLevel,
}

#[derive(serde::Serialize, Clone)]
struct MessageLogPayload {
    timestamp: String,
    message: String,
    tag: usize,
}
#[derive(serde::Serialize)]
struct Message<T> {
    action: String,
    data: T,
}

#[derive(serde::Serialize)]
struct UpdateDockerJobWorkflowStateMessagePayload {
    id: i32,
    workflow_state: String,
}

#[derive(serde::Serialize)]
struct DockerJobResourceUsagePayload {
    id: i32,
    cpu_seconds: f64,
    current_cpu: f64,
    ram_gb_seconds: f64,
    current_ram_gb: f64,
    net_rx_gb: f64,
    current_net_rx_gb: f64,
    net_tx_gb: f64,
    current_net_tx_gb: f64,
    total_cost: f64,
}

#[derive(serde::Deserialize)]
struct WOBMessage {
    wob_id: i32,
    wob_type: String,
}

async fn monitor_resources_and_logs(
    config: MirandaConfig,
    docker_job_id: i32,
    wob_id: i32, // replace with actual type of msg
    rtmsg_ticket: String,
    bucket_long_interval: u64,
) -> Result<Option<Box<RatelimitedLogPusher>>, Error> {
    let lscn_rtmsg_ticket = rtmsg_ticket.clone();
    let mut sc = mirmod_rs::sctx::SecurityContext::new_from_config(config.clone())
        .await
        .expect("Failed to create security context");
    sc.renew_id().await.expect("Failed to renew ID");
    // Spawn the first task for resource monitoring
    tokio::spawn(async move {
        let mut rmon_sc = mirmod_rs::sctx::SecurityContext::new_from_config(config.clone())
            .await
            .expect("Failed to create security context");
        rmon_sc.renew_id().await.expect("Failed to renew ID");
        let mut sys = System::new_all();
        let mut docker_job = mirmod_rs::orm::find_by_id::<mirmod_rs::orm::docker_job::DockerJob>(
            &mut rmon_sc,
            docker_job_id,
        )
        .await
        .expect("Failed to find docker job");

        let crg: Option<mirmod_rs::orm::crg::ComputeResourceGroup> = match docker_job.crg_id() {
            Some(crg_id) => Some(
                mirmod_rs::orm::find_by_id::<mirmod_rs::orm::crg::ComputeResourceGroup>(
                    &mut rmon_sc,
                    crg_id,
                )
                .await
                .expect("Failed to find compute resource group"),
            ),
            None => None,
        };

        let mut last_report = Instant::now();
        let scan_rate = Duration::from_millis(1000);
        let report_rate = Duration::from_millis(10000);
        let second_scaling_factor: f64 = 1.0 / scan_rate.as_secs_f64();
        let mut total_cpu_usage: f64 = 0.0;
        let mut total_mem_usage: f64 = 0.0;
        let mut total_net_tx: f64 = 0.0;
        let mut total_net_rx: f64 = 0.0;
        let mut total_cost: f64 = 0.0;

        sys.refresh_all();
        tokio::time::sleep(scan_rate).await;

        loop {
            sys.refresh_cpu();
            sys.refresh_memory();
            sys.refresh_networks();
            for cpu in sys.cpus() {
                // divide by 100 to convert % to cpu seconds
                total_cpu_usage += cpu.cpu_usage() as f64 * second_scaling_factor / 100.0;
            }
            for (interface_name, data) in sys.networks() {
                if interface_name != "en0" && interface_name != "eth0" {
                    continue;
                }
                total_net_tx += data.transmitted() as f64 / 1024.0 / 1024.0 / 1024.0;
                total_net_rx += data.received() as f64 / 1024.0 / 1024.0 / 1024.0;
            }
            let current_mem_usage_gb = sys.used_memory() as f64 / 1024.0 / 1024.0 / 1024.0;
            total_mem_usage += current_mem_usage_gb * second_scaling_factor;

            if last_report.elapsed() > report_rate {
                let mut update_data = DockerJobResourceUsagePayload {
                    id: docker_job_id,
                    cpu_seconds: total_cpu_usage,
                    current_cpu: 0.0,
                    ram_gb_seconds: total_mem_usage,
                    current_ram_gb: 0.0,
                    net_tx_gb: total_net_tx,
                    current_net_tx_gb: 0.0,
                    net_rx_gb: total_net_rx,
                    current_net_rx_gb: 0.0,
                    total_cost,
                };
                docker_job = mirmod_rs::orm::find_by_id::<mirmod_rs::orm::docker_job::DockerJob>(
                    &mut rmon_sc,
                    docker_job.id(),
                )
                .await
                .expect("Failed to find docker job");

                if let Some(crg) = &crg {
                    let seconds_since_last_report = last_report.elapsed().as_secs_f64();
                    last_report = Instant::now();

                    update_data.current_cpu = (total_cpu_usage - docker_job.cpu_seconds() as f64)
                        / seconds_since_last_report;
                    docker_job.set_current_cpu(update_data.current_cpu as f32);
                    update_data.current_ram_gb = (total_mem_usage
                        - docker_job.ram_gb_seconds() as f64)
                        / seconds_since_last_report;
                    docker_job.set_current_ram_gb(update_data.current_ram_gb as f32);
                    update_data.current_net_tx_gb =
                        (total_net_tx - docker_job.net_tx_gb() as f64) / seconds_since_last_report;
                    docker_job.set_current_net_tx_gb(update_data.current_net_tx_gb as f32);
                    update_data.current_net_rx_gb =
                        (total_net_rx - docker_job.net_rx_gb() as f64) / seconds_since_last_report;
                    docker_job.set_current_net_rx_gb(update_data.current_net_rx_gb as f32);

                    let credit_cost = (update_data.current_cpu / 3600.0
                        * crg.cost_per_cpu_hour().to_f64().unwrap())
                        + (update_data.current_ram_gb / 3600.0
                            * crg.cost_per_gb_hour().to_f64().unwrap())
                        + (update_data.current_net_tx_gb
                            * crg.cost_per_net_tx_gb().to_f64().unwrap())
                        + (update_data.current_net_rx_gb
                            * crg.cost_per_net_rx_gb().to_f64().unwrap());
                    total_cost += credit_cost;
                    update_data.total_cost = total_cost;
                    docker_job.set_total_cost(total_cost as f32);

                    println!("📜 [INFO] 💸 d_cpu[{:.2}cs], d_mem:[{:.4}gbs], d_net[^{:.4}gb, v{:.4}gb], cost:{:.4} + {:.4}",
                        update_data.current_cpu, update_data.current_ram_gb, update_data.current_net_tx_gb, update_data.current_net_rx_gb, total_cost, credit_cost);

                    let statement = format!(
                        "{:.4},CPU:{:.4},MEM:{:.4},NTX:{:.4},NRX:{:.4}",
                        docker_job.id(),
                        update_data.current_cpu,
                        update_data.current_ram_gb,
                        update_data.current_net_tx_gb,
                        update_data.current_net_rx_gb
                    );

                    if credit_cost > 0.0 {
                        if let Ok(amount) = mirmod_rs::orm::BigDecimal::try_from(credit_cost) {
                            match mirmod_rs::orm::transact_credits(&mut rmon_sc, amount, &statement)
                                .await
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("📜 Failed to transact credits: {}", e);
                                    println!("📜 Killing the process.");
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    last_report = Instant::now();
                }

                println!(
                    "📜 [INFO] 📈 cpu[{:.2}cs], mem:[{:.4}gbs], net[^{:.4}gb, v{:.4}gb]",
                    total_cpu_usage, total_mem_usage, total_net_tx, total_net_rx
                );

                match docker_job.workflow_state() {
                    mirmod_rs::orm::docker_job::WorkflowState::Exited => {
                        println!("📜 docker job exited, stopping resource monitor");
                        break;
                    }
                    mirmod_rs::orm::docker_job::WorkflowState::Error => {
                        println!("📜 docker job errored, stopping resource monitor");
                        break;
                    }
                    _ => {}
                }

                rmon_sc.extend_proxy_account_claim().await.ok();

                docker_job.set_cpu_seconds(total_cpu_usage as f32);
                docker_job.set_ram_gb_seconds(total_mem_usage as f32);
                docker_job.set_net_rx_gb(total_net_rx as f32);
                docker_job.set_net_tx_gb(total_net_tx as f32);
                mirmod_rs::orm::update(&mut rmon_sc, &mut docker_job)
                    .await
                    .ok();
                mirmod_rs::orm::RealtimeMessage::send_to_ko(
                    &mut rmon_sc,
                    wob_id,
                    lscn_rtmsg_ticket.clone(),
                    serde_json::to_string(&Message {
                        action: "update[DOCKER_JOB]".into(),
                        data: update_data,
                    })
                    .unwrap(),
                )
                .await
                .ok();
            }
            tokio::time::sleep(scan_rate).await;
        }
        println!("📜 resource monitor done, ensuring workflow_state = WorkflowState::Exited");
        loop {
            docker_job.set_workflow_state(mirmod_rs::orm::docker_job::WorkflowState::Exited);
            match mirmod_rs::orm::update(&mut rmon_sc, &mut docker_job).await {
                Ok(_) => break,
                Err(e) => {
                    println!("📜 Failed to update docker job: {}", e);
                }
            }
            println!("📜 Retrying in 10s");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        std::process::exit(0);
    });

    // Channel for log processing
    let (tx, mut rx) = tokio::sync::mpsc::channel(100)
        as (
            tokio::sync::mpsc::Sender<Vec<LogLine>>,
            tokio::sync::mpsc::Receiver<Vec<LogLine>>,
        );
    tokio::spawn(async move {
        while let Some(loglines) = rx.recv().await {
            let mut msg_groups = Vec::new();
            let mut buff_line: Option<MessageLogPayload> = None;
            let mut chunk = Vec::new();
            for log in &loglines {
                print!("{}", log.line);
                let payload = MessageLogPayload {
                    timestamp: log
                        .timestamp
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    message: log.line.clone(),
                    tag: log.level as usize,
                };

                if let Some(buff) = &mut buff_line {
                    if buff.to_owned().tag != payload.tag {
                        msg_groups.push(buff.clone());
                        buff_line = Some(payload.clone());
                    } else {
                        buff.message.push_str(&payload.message);
                    }
                } else {
                    buff_line = Some(payload.clone());
                }

                chunk.push(payload);
            }
            if let Some(buff) = buff_line {
                msg_groups.push(buff);
            }

            let mut payloads = Vec::new();
            recurse_break_logs(vec![chunk], &mut payloads, 0);
            for payload in payloads {
                let encoded = serde_json::to_string(&Message {
                    action: format!("logs[{}]", docker_job_id),
                    data: payload,
                })
                .unwrap();
                let send_res = mirmod_rs::orm::RealtimeMessage::send_to_ko(
                    &mut sc,
                    wob_id,
                    rtmsg_ticket.clone(),
                    encoded,
                )
                .await;

                if let Err(e) = send_res {
                    println!("📜 Failed to send logs: {}", e);
                }
            }

            for msg in msg_groups {
                mirmod_rs::orm::MirandaLog::create(
                    &mut sc,
                    msg.message,
                    msg.tag as i64,
                    mirmod_rs::orm::MirandaClasses::DockerJob,
                    docker_job_id.into(),
                )
                .await
                .ok();
            }
        }
    });
    let log_pusher = Some(Box::new(RatelimitedLogPusher::new(
        Duration::from_millis(bucket_long_interval),
        tx,
    )));
    Ok(log_pusher)
}

#[tokio::main]
async fn main() {
    println!("📜 logzod");
    let token = std::env::var("WOB_TOKEN").expect("Expected a token in the environment");
    let docker_job_id = std::env::var("DOCKER_JOB_ID")
        .expect("Expected a docker job id in the environment")
        .parse::<i32>()
        .expect("Failed to parse docker job id as i32");

    let msg: WOBMessage = serde_json::from_str(
        &std::env::var("WOB_MESSAGE").expect("Expected a message in the environment"),
    )
    .expect("Failed to parse message");

    if msg.wob_type != "KNOWLEDGE_OBJECT" {
        println!("📜 Invalid wob type: {}", msg.wob_type);
        return;
    }

    let rtmsg_ticket =
        std::env::var("REALTIME_MESSAGE_TICKET").expect("Expected a ticket in the environment");

    let bucket_long_interval = std::env::var("LOG_BUCKET_LONG_INTERVAL")
        .unwrap_or("250".into())
        .parse::<u64>()
        .expect("Failed to parse bucket long interval as u64");

    let config = mirmod_rs::config::MirandaConfig::new_from_default()
        .expect("Failed to load default config from system paths")
        .merge_into_new(
            mirmod_rs::config::PartialMirandaConfig::new_from_token_string(token).expect(
                "Failed to load partial config from token string (this should never happen)",
            ),
        )
        .expect("Failed to merge configs");
    let mut sc = mirmod_rs::sctx::SecurityContext::new_from_config(config.clone())
        .await
        .expect("Failed to create security context");
    sc.renew_id().await.expect("Failed to renew ID");

    let mut ob =
        mirmod_rs::orm::find_by_id::<mirmod_rs::orm::docker_job::DockerJob>(&mut sc, docker_job_id)
            .await
            .expect("Failed to find docker job");

    ob.set_workflow_state(mirmod_rs::orm::docker_job::WorkflowState::Starting);
    mirmod_rs::orm::update(&mut sc, &mut ob)
        .await
        .expect("Failed to update docker job");
    mirmod_rs::orm::RealtimeMessage::send_to_ko(
        &mut sc,
        msg.wob_id,
        rtmsg_ticket.clone(),
        serde_json::to_string(&Message {
            action: "update[DOCKER_JOB]".into(),
            data: UpdateDockerJobWorkflowStateMessagePayload {
                id: docker_job_id,
                workflow_state: ob.workflow_state().as_str(),
            },
        })
        .unwrap(),
    )
    .await
    .ok();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut quit = false;
    println!("📜 Spawining new processor.");
    let mut log_pusher: Option<Box<RatelimitedLogPusher>> = None;
    loop {
        let cmd = std::process::Command::new(&args[0])
            .args(&args[1..])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();

        let mut proc = match cmd {
            Ok(cmd) => cmd,
            Err(e) => {
                println!("📜 Failed to spawn command: {}", e);
                return;
            }
        };

        let stdout = proc.stdout.take().expect("Failed to take stdout");
        let stderr = proc.stderr.take().expect("Failed to take stderr");

        let mut stdout_reader = std::io::BufReader::new(stdout);
        let mut stderr_reader = std::io::BufReader::new(stderr);

        // resource usage monitor
        let rmon_rtmsg_ticket = rtmsg_ticket.clone();
        if log_pusher.is_none() {
            let result = monitor_resources_and_logs(
                config.clone(),
                docker_job_id,
                msg.wob_id,
                rmon_rtmsg_ticket,
                bucket_long_interval,
            )
            .await
            .expect("Failed to start resource monitor.");

            log_pusher.get_or_insert_with(|| {
                result.expect("Failed to setup the log pusher.") // Assign the result to log_pusher
            });
        }
        let mut line = String::new();
        // let mut pusher = log_pusher.unwrap();
        loop {
            match stdout_reader.read_line(&mut line) {
                Ok(0) => break, // End of file reached, break the loop
                Ok(_) => {
                    log_pusher
                        .as_mut()
                        .unwrap()
                        .log(line.clone(), match_line_to_tag(&line))
                        .await;
                    line.clear(); // Clear the line for the next input
                }
                Err(e) => {
                    println!("📜 Failed to read line: {}", e);
                    break;
                }
            }
        }

        println!("📜 stdout done");
        let mut dont_get_stderr = false;
        // wait for the process to exit
        match proc.wait() {
            Ok(status) => {
                println!("📜 Child Process exited with status: {}", status);
                match status.code() {
                    Some(0) => {
                        ob.set_workflow_state(mirmod_rs::orm::docker_job::WorkflowState::Exited);
                        quit = true;
                    }
                    Some(200) => {
                        println!("📜 Process requested restart.");
                        dont_get_stderr = true;
                    }
                    _ => ob.set_workflow_state(mirmod_rs::orm::docker_job::WorkflowState::Error),
                }
            }
            Err(e) => {
                println!("📜 Failed to wait for child process: {}", e);
                ob.set_workflow_state(mirmod_rs::orm::docker_job::WorkflowState::Error);
            }
        }

        if !dont_get_stderr {
            loop {
                match stderr_reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        log_pusher
                            .as_mut()
                            .unwrap()
                            .log(line.clone(), LogLevel::Warning)
                            .await;
                        line.clear();
                    }
                    Err(e) => {
                        println!("📜 Failed to read line: {}", e);
                        break;
                    }
                }
            }
        }

        println!("📜 stderr done");

        if let Some(handle) = log_pusher.as_mut().unwrap().handle.take() {
            loop {
                if handle.is_finished() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if quit {
            break;
        }
        println!("📜 Respawning processor.");
    }

    // the security context was moved to a new thread, so we need to create a new one to update the docker job
    let mut sc = mirmod_rs::sctx::SecurityContext::new_from_config(config)
        .await
        .expect("Failed to create security context");
    sc.renew_id().await.expect("Failed to renew ID");
    if ob.workflow_state() != mirmod_rs::orm::docker_job::WorkflowState::Error {
        ob.set_workflow_state(mirmod_rs::orm::docker_job::WorkflowState::Exited);
    }
    mirmod_rs::orm::update(&mut sc, &mut ob)
        .await
        .expect("Failed to update docker job");
    mirmod_rs::orm::RealtimeMessage::send_to_ko(
        &mut sc,
        msg.wob_id,
        rtmsg_ticket,
        serde_json::to_string(&Message {
            action: "update[DOCKER_JOB]".into(),
            data: UpdateDockerJobWorkflowStateMessagePayload {
                id: docker_job_id,
                workflow_state: ob.workflow_state().as_str(),
            },
        })
        .unwrap(),
    )
    .await
    .ok();

    println!("📜 done");
}

// logs should be pushed to the server every 250ms. they should build up in a Vec<String>.
// when a new line is pushed, check if the last time a log was pushed was > 250ms ago.
// if it was, immediately send the logs over a channel to the main thread.
// if it wasn't, restart a 250ms tokio timeout timer.
// when the timer expires, send the logs over a channel to the main thread.

struct RatelimitedLogPusherSharedState {
    collected: Vec<LogLine>,
    last_push: Instant,
    tx: mpsc::Sender<Vec<LogLine>>,
}

struct RatelimitedLogPusher {
    handle: Option<JoinHandle<()>>,
    long_push_interval: Duration,
    shared_state: Arc<Mutex<RatelimitedLogPusherSharedState>>, // Propagate the lifetime here
}

impl RatelimitedLogPusher {
    fn new(long_push_interval: Duration, tx: mpsc::Sender<Vec<LogLine>>) -> Self {
        Self {
            long_push_interval,
            handle: None,
            shared_state: Arc::new(Mutex::new(RatelimitedLogPusherSharedState {
                collected: Vec::new(),
                last_push: Instant::now(),
                tx,
            })),
        }
    }

    async fn push_logs(&mut self) {
        self.abort();
        Self::send(&self.shared_state).await;
    }

    async fn reset_timeout(&mut self, push_interval: Duration) {
        self.abort();
        let shared_state = Arc::clone(&self.shared_state);
        self.handle = Some(tokio::spawn(async move {
            tokio::time::sleep(push_interval).await;
            Self::send(&shared_state).await;
        }));
    }

    async fn log(&mut self, line: String, level: LogLevel) {
        let elapsed = {
            let mut state = self.shared_state.lock().await;
            state.collected.push(LogLine {
                timestamp: chrono::Utc::now(),
                line,
                level,
            });

            state.last_push.elapsed()
        };

        if elapsed > self.long_push_interval {
            self.push_logs().await;
        } else {
            self.reset_timeout(self.long_push_interval).await;
        }
    }

    fn abort(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }

    async fn send(state: &Mutex<RatelimitedLogPusherSharedState>) {
        let mut state = state.lock().await;
        let logs = state.collected.drain(..).collect::<Vec<_>>();

        if !logs.is_empty() {
            state.tx.send(logs).await.expect("Failed to send logs");
            state.last_push = Instant::now();
        }
    }
}

fn recurse_break_logs(
    chunks: Vec<Vec<MessageLogPayload>>,
    result: &mut Vec<Vec<MessageLogPayload>>,
    depth: i32,
) {
    if depth > 10 {
        println!(
            "[LOG COLLECTOR] could not break logs into 15kb byte chunks after {} tries",
            depth
        );
        return;
    }
    for chunk in chunks {
        let enclen = rough_json_enc_estimate(&chunk);
        // 15360 is the max size of a message payload, we target 100 bytes under that to be safe
        if enclen > 15260 {
            if chunk.len() == 1 {
                let log = &chunk[0];
                result.push(vec![MessageLogPayload {
                    timestamp: log.timestamp.clone(),
                    tag: log.tag,
                    message: format!("{}... <log too long>", &log.message[..15260]),
                }]);
                continue;
            }
            let mid = chunk.len() / 2;
            recurse_break_logs(
                vec![chunk[..mid].to_vec(), chunk[mid..].to_vec()],
                result,
                depth + 1,
            );
        } else {
            result.push(chunk);
        }
    }
}

fn rough_json_enc_estimate(payload: &Vec<MessageLogPayload>) -> usize {
    let mut result = 0;
    for log in payload {
        // 38 was derived enc.len() - (log.timestamp.len() + log.message.len())
        result += log.timestamp.len() + log.message.len() + 38;
    }
    result
}
