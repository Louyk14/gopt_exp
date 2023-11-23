import os

with open("analyze_result.log", "w") as fw:
	for main_dir, sub_dir, file_name_list in os.walk("results"):
		sub_dir.sort()
		for name in sorted(file_name_list):
			file_name = os.path.join(main_dir, name)
			if file_name.split('.')[-1] != "log":
				continue
			
			file_name = file_name.strip()
			t_sum = 0
			t_num = 0
			with open(file_name, "r") as f:
				lines = f.readlines()
				for line in lines:
					line = line.strip().split(' ')[0]
					t_cost = int(line)
					t_sum += t_cost
					t_num += 1
					
			fw.write(file_name + " " + str(t_sum / t_num) + "\n")

