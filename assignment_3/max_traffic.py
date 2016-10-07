import operator
from os import listdir
from os.path import isfile, join
onlyfiles = [join('traffic', f) for f in listdir('traffic') if isfile(join('traffic', f))]
class Point:
    def __init__(self, point,count):
        self.point = point
        #self.avg_time = 0
        #self.avg_speed = 0
        self.vehicle_count = count
  
    def __str__(self):
        a = ['point : '+str(self.point),  'vehicle_count : '+str(self.vehicle_count)]
        return '\n'.join(a)

report_ids = []
vehicle_count = []

for i in onlyfiles:
    report_ids.append(i.split('trafficData')[1].split('.csv')[0])
    veh_count = 0
 
    txt = open(i, 'r').read()
    txt = txt.split('\n')
   
    txt = txt[1:-1]

    for f in txt:
        veh_count = veh_count + int(f.split(',')[6])

    vehicle_count.append(veh_count)

obj = [Point(report_ids[i],vehicle_count[i]) for i in range(0,len(report_ids))]

max_traffic = 0
max_point = None
for o in obj:
    if o.vehicle_count > max_traffic :
        max_traffic = o.vehicle_count
        max_point = o

print(max_point)
