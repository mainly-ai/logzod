use std::io::BufRead;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{CpuExt, NetworkExt, NetworksExt, ProcessExt, System, SystemExt};
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
    if line.starts_with("[INFO]") {
        return LogLevel::Info;
    } else if line.starts_with("[WARN]") {
        return LogLevel::Warning;
    } else if line.starts_with("[ERROR]") {
        return LogLevel::Error;
    } else if line.starts_with("[DEBUG]") || line.starts_with("|=>") {
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
struct UpdateDockerJobResourceUsagePayload {
    id: i32,
    cpu_seconds: f32,
    ram_gb_seconds: f32,
    net_tx_gb: f32,
    net_rx_gb: f32,
}

#[tokio::main]
async fn main() {
    println!("📜 logzod");
    let token = std::env::var("WOB_TOKEN").expect("Expected a token in the environment");
    let docker_job_id = std::env::var("DOCKER_JOB_ID")
        .expect("Expected a docker job id in the environment")
        .parse::<i32>()
        .expect("Failed to parse docker job id as i32");

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

    ob.set_workflow_state(mirmod_rs::orm::docker_job::DockerJobWorkflowState::Starting);
    mirmod_rs::orm::update(&mut sc, &mut ob)
        .await
        .expect("Failed to update docker job");
    mirmod_rs::orm::RealtimeMessage::send_to_self(
        &mut sc,
        serde_json::to_string(&Message {
            action: "update[DOCKER_JOB]".into(),
            data: UpdateDockerJobWorkflowStateMessagePayload {
                id: docker_job_id,
                workflow_state: ob.workflow_state.as_str().into(),
            },
        })
        .unwrap(),
    )
    .await
    .ok();

    let args: Vec<String> = std::env::args().skip(1).collect();
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
    let mut rmon_sc = mirmod_rs::sctx::SecurityContext::new_from_config(config.clone())
        .await
        .expect("Failed to create security context");
    rmon_sc.renew_id().await.expect("Failed to renew ID");
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut docker_job = mirmod_rs::orm::find_by_id::<mirmod_rs::orm::docker_job::DockerJob>(
            &mut rmon_sc,
            docker_job_id,
        )
        .await
        .expect("Failed to find docker job");

        let mut last_report = Instant::now();
        let scan_rate = Duration::from_millis(1000);
        let report_rate = Duration::from_millis(5000);
        let second_scaling_factor: f32 = 1.0 / scan_rate.as_secs_f32();
        let mut total_cpu_usage: f32 = 0.0;
        let mut total_mem_usage: f32 = 0.0;
        let mut total_net_tx: f32 = 0.0;
        let mut total_net_rx: f32 = 0.0;

        sys.refresh_all();
        tokio::time::sleep(scan_rate).await;

        loop {
            sys.refresh_cpu();
            sys.refresh_memory();
            sys.refresh_networks();
            for cpu in sys.cpus() {
                // divide by 100 to convert % to cpu seconds
                total_cpu_usage += cpu.cpu_usage() * second_scaling_factor / 100.0;
            }
            for (interface_name, data) in sys.networks() {
                if interface_name != "en0" && interface_name != "eth0" {
                    continue;
                }
                total_net_tx += data.transmitted() as f32 / 1024.0 / 1024.0 / 1024.0;
                total_net_rx += data.received() as f32 / 1024.0 / 1024.0 / 1024.0;
            }
            let current_mem_usage_gb = sys.used_memory() as f32 / 1024.0 / 1024.0 / 1024.0;
            total_mem_usage += current_mem_usage_gb * second_scaling_factor;
            if last_report.elapsed() > report_rate {
                last_report = Instant::now();
                println!(
                    "📜 [INFO] cpu[{:.2}cs], mem:[{:.4}gbs], net[^{:.4}gb, v{:.4}gb]",
                    total_cpu_usage, total_mem_usage, total_net_tx, total_net_rx
                );
                docker_job.set_cpu_seconds(total_cpu_usage);
                docker_job.set_ram_gb_seconds(total_mem_usage);
                mirmod_rs::orm::update(&mut rmon_sc, &mut docker_job)
                    .await
                    .ok();
                mirmod_rs::orm::RealtimeMessage::send_to_self(
                    &mut rmon_sc,
                    serde_json::to_string(&Message {
                        action: "update[DOCKER_JOB]".into(),
                        data: UpdateDockerJobResourceUsagePayload {
                            id: docker_job_id,
                            cpu_seconds: total_cpu_usage,
                            ram_gb_seconds: total_mem_usage,
                            net_tx_gb: total_net_tx,
                            net_rx_gb: total_net_rx,
                        },
                    })
                    .unwrap(),
                )
                .await
                .ok();
            }
            tokio::time::sleep(scan_rate).await;
        }
    });

    let (tx, mut rx) =
        mpsc::channel(100) as (mpsc::Sender<Vec<LogLine>>, mpsc::Receiver<Vec<LogLine>>);

    tokio::spawn(async move {
        while let Some(loglines) = rx.recv().await {
            let mut msg_groups = Vec::new();
            let mut buff_line: std::option::Option<MessageLogPayload> = None;
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
                let send_res =
                    mirmod_rs::orm::RealtimeMessage::send_to_self(&mut sc, encoded).await;

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

    let mut log_pusher = RatelimitedLogPusher::new(Duration::from_millis(bucket_long_interval), tx);

    let mut line = String::new();

    loop {
        match stdout_reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                log_pusher.log(line.clone(), match_line_to_tag(&line)).await;
                line.clear();
            }
            Err(e) => {
                println!("📜 Failed to read line: {}", e);
                break;
            }
        }
    }

    println!("📜 stdout done");

    // wait for the process to exit
    match proc.wait() {
        Ok(status) => {
            println!("📜 Child Process exited with status: {}", status);
            ob.set_workflow_state(mirmod_rs::orm::docker_job::DockerJobWorkflowState::Exited);
        }
        Err(e) => {
            println!("📜 Failed to wait for child process: {}", e);
            ob.set_workflow_state(mirmod_rs::orm::docker_job::DockerJobWorkflowState::Error);
        }
    }

    loop {
        match stderr_reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                log_pusher.log(line.clone(), LogLevel::Warning).await;
                line.clear();
            }
            Err(e) => {
                println!("📜 Failed to read line: {}", e);
                break;
            }
        }
    }

    println!("📜 stderr done");

    if let Some(handle) = log_pusher.handle.take() {
        loop {
            if handle.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // the security context was moved to a new thread, so we need to create a new one to update the docker job
    let mut sc = mirmod_rs::sctx::SecurityContext::new_from_config(config)
        .await
        .expect("Failed to create security context");
    sc.renew_id().await.expect("Failed to renew ID");
    let mut ob =
        mirmod_rs::orm::find_by_id::<mirmod_rs::orm::docker_job::DockerJob>(&mut sc, docker_job_id)
            .await
            .expect("Failed to find docker job");
    if ob.workflow_state != mirmod_rs::orm::docker_job::DockerJobWorkflowState::Error {
        ob.set_workflow_state(mirmod_rs::orm::docker_job::DockerJobWorkflowState::Exited);
    }
    mirmod_rs::orm::update(&mut sc, &mut ob)
        .await
        .expect("Failed to update docker job");
    mirmod_rs::orm::RealtimeMessage::send_to_self(
        &mut sc,
        serde_json::to_string(&Message {
            action: "update[DOCKER_JOB]".into(),
            data: UpdateDockerJobWorkflowStateMessagePayload {
                id: docker_job_id,
                workflow_state: ob.workflow_state.as_str().into(),
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
    shared_state: Arc<Mutex<RatelimitedLogPusherSharedState>>,
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
