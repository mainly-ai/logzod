import time
import sys

j = 0
while j < 4:
	i = 0
	while i < 250:
		if i == 100:
			print('[DEBUG] ', end='')
		print(f"[{i+1}/25] {time.time()} Hello from Python, {sys.argv[1]}!")
		time.sleep(0.01)
		i += 1
	j += 1
	print(f'zzzz {j}')
	raise Exception('test')
	time.sleep(1)