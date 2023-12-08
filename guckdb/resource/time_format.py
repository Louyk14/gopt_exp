from datetime import datetime
import os
import re

time_dict = dict()
table_map = dict()

with open("schema.sql", "r") as f:
	lines = f.readlines()
	counter = 0
	table_name = ""
	for line in lines:
		if "create table" in line:
			counter = 0
			table_name = line.strip().split(" ")[2]
		else:
			if "timestamp" in line:
				time_dict[table_name] = counter
			counter += 1

with open("load.sql", "r") as f:
	lines = f.readlines()
	for line in lines:
		line = re.sub(' +', ' ', line.strip())
		line_split = line.strip().split(' ')

		if line_split[0] == "COPY" and line_split[2] == "FROM":
			table_name = line_split[1]
			file_name = line_split[3].split('/')[-1][:-1]
			table_map[file_name] = table_name
		elif line_split[0] == "COPY" and line_split[5] == "FROM":
			table_name = line_split[1]
			file_name = line_split[6].split('/')[-1][:-1]
			table_map[file_name] = table_name



for main_dir, sub_dir, file_name_list in os.walk("sf01"):
	for name in file_name_list:
		file_name = os.path.join(main_dir, name)
		if file_name.split('.')[-1] != "csv":
			continue
		
		file = file_name.strip().split('/')[-1]
		
		if file not in table_map:
			continue
		table_name = table_map[file]
		print(table_name, table_name in time_dict)
		if table_name in time_dict:	
			index = time_dict[table_name]
			with open(file, "w") as fw:
				with open(file_name, "r") as f:
					lines = f.readlines()
					for i in range(1, len(lines)):
						line = lines[i]
						line = line.strip().split('|')
						line[index] = str(int(datetime.strptime(line[index],"%Y-%m-%dT%H:%M:%S.%f+0000").timestamp() * 1000))
						line = '|'.join(line)
						lines[i] = line
					for line in lines:
						fw.write(line.strip() + "\n")







