import os,sys,csv


def WriteDictToCSV(csv_columns,dict_data):
    try:
        with open('player_vs_player.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError as (errno, strerror):
            print("I/O error({0}): {1}".format(errno, strerror))    
    return  

#output_file=open('player_vs_player.csv','w')
#csvwriter=csv.writer(output_file)

player_vs_player={}
for _, _, f in os.walk('Dataset'):
	for i in f:
		x = open(os.path.curdir + '/Dataset/'+i)
		c=0

		for line in x.readlines():
			details = line.split(',')
			if details[0].strip()=='ball':
				players1=(details[4],details[6])
				players2=(details[5],details[6])

				if players1 in player_vs_player.keys():
					player_vs_player[players1]['total']+=int(details[7])
					player_vs_player[players1]['runs'][int(details[7])]+=1
					player_vs_player[players1]['balls']+=1
					if details[9]!='""' and details[9]!='run out' and players1[0].strip()==details[10].strip():
						player_vs_player[players1]['wickets']+=1
				else:
					player_vs_player[players1]={}
					player_vs_player[players1]['total']=int(details[7])
					player_vs_player[players1]['runs']=[0,0,0,0,0,0,0,0]
					player_vs_player[players1]['wickets']=0
					player_vs_player[players1]['balls']=1
					if details[9]!='""' and details[9]!='run out' and players1[0].strip()==details[10].strip():
						player_vs_player[players1]['wickets']+=1

				if players2 in player_vs_player.keys():
					player_vs_player[players2]['total']+=int(details[8])
					player_vs_player[players2]['runs'][int(details[8])]+=1
					player_vs_player[players2]['balls']+=1
					if details[9]!='""' and details[9]!='run out' and players2[0].strip()==details[10].strip():
						player_vs_player[players2]['wickets']+=1
				else:
					player_vs_player[players2]={}
					player_vs_player[players2]['total']=int(details[8])
					player_vs_player[players2]['runs']=[0,0,0,0,0,0,0,0]
					player_vs_player[players2]['wickets']=0
					player_vs_player[players2]['balls']=1
					if details[9]!='""' and details[9]!='run out' and players2[0].strip()==details[10].strip():
						player_vs_player[players2]['wickets']+=1

csv_columns = ['Batsman','Bowler','Total','0s','1s','2s','3s','4s','5s','6s','7s','Balls','Wickets']
data=[]
for i in player_vs_player.keys():
	data.append({'Batsman':i[0],'Bowler':i[1],'Total':player_vs_player[i]['total'],'0s':player_vs_player[i]['runs'][0],'1s':player_vs_player[i]['runs'][1],'2s':player_vs_player[i]['runs'][2],'3s':player_vs_player[i]['runs'][3],'4s':player_vs_player[i]['runs'][4],'5s':player_vs_player[i]['runs'][5],'6s':player_vs_player[i]['runs'][6],'7s':player_vs_player[i]['runs'][7],'Balls':player_vs_player[i]['balls'],'Wickets':player_vs_player[i]['wickets']})

WriteDictToCSV(csv_columns,data)

	

