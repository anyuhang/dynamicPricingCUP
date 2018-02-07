import urllib

def read_sentences(handle):
 for l in handle:
   if l.strip():
     yield l.strip()

def readfile(fn):
  s = []
  for sentence in read_sentences(open(fn)):
      s.append(sentence.split(','))
  return s

def getQuery(dict, geoline):
  kw = []
  for i in range(len(geoline)):
    pa =  dict[geoline[i][2]]
    for j in range(len(pa)):
      kw.append([geoline[i][0], pa[j]+'+attorney+'+geoline[i][3]])
      kw.append([geoline[i][0], pa[j]+'+lawyer+'+geoline[i][3]])
      kw.append([geoline[i][0], pa[j]+'+attorney+'+geoline[i][4]])
      kw.append([geoline[i][0], pa[j]+'+lawyer+'+geoline[i][4]])
  return kw

def runQuery(qry, kw):
    output = []
    for i in kw:
        query = qry + i[1]
        try:
          resp = urllib.urlopen(query).read()
          semRushRst = []
          semRushRst.extend(i)
          semRushRst.extend(resp.split(';')[5:])
          output.append(semRushRst)
        except:
          print i
          pass
    return output

def 2file(outputFile, outputList):
  output = open(outputFile, 'w')
  for item in outputList:
    output.("%s\n" % item)
  output.close()

fn = '/Users/yan/Code/semrush/dp_mkt_geo.csv'
qry = 'http://api.semrush.com/?type=phrase_this&key=apikey&export_columns=Ph,Nq,Cp,Co,Nr&database=us&phrase='
geoline = readfile(fn)   --geoline=geoline[1:]
kw = getQuery(dict,geoline)
outputList = runQuery(qry,kw)
2file('semRushResult.csv', outputList)

qry = 'http://api.semrush.com/?type=phrase_related&key=apikey&display_limit=10&export_columns=Ph,Nq,Cp,Co,Nr,Td&database=us&phrase='
query = qry + 'car+accidents+seattle+lawyer'

---- related keyword experiments
dict_pa = {'Bankruptcy & Debt': ['bankruptcy'],
'Car Accidents':['car+accidents'],
'Child Custody':['joint+custody','child +custody'],
'Criminal Defense':['criminal+defense'],
'DUI & DWI':['dui','dwi'],
'Divorce & Separation':['divorce'],
'Domestic Violence':['domestic+violence'],
'Employment & Labor':['eeoc','employment'],
'Family':['family'],
'Immigration':['immigration'],
'Medical Malpractice':['medical+malpractice'],
'Personal Injury':['personal+injury'],
'Probate':['probate+will','probate'],
'Speeding & Traffic Ticket':['traffic+ticket','speeding+ticket'],
'Workers Compensation':['workers+compensation']}

list_geo = ['harris', 'seattle', 'los+angeles']
def getQuery(dict_pa, list_geo):
    kw = []
    for i in range(len(list_geo)):
      geo =  list_geo[i]
      for key in dict_pa:
          for values in dict_pa[key]:
            kw.append([geo, key, values+'+'+'attorney'+'+'+geo])
    return kw

def runQuery(qry, kw):
    output = []
    for i in kw:
        query = qry + i[2]
        try:
          print i
          resp = urllib.urlopen(query).read()
          semRushRst = resp.split('\r\n')[1:]
          output.append([semRushRst,i])
        except:
          print 'bad ' + i
          pass
    return output

kw = getQuery(dict_pa, list_geo)
rst = runQuery(qry, kw)

rst_clean=[]
for i in rst:
   if len(i[0])>0:
     rst_clean.append(i)

rst_clean=[]
for i in rst:
  if len(i[0])>0:
    for k in i[0]:
      line = k.split(';')
      if line[2]>0:
        rst_clean.append([line[0],line[1],line[2], i[1]])

rst_clean_avg=[]
for i in rst:
  if len(i[0])>0:
    sum = 0
    cnt = 0
    for k in i[0]:
      line = k.split(';')
      if line[2]>0:
        sum += int(line[1]) * float(line[2])
        cnt += int(line[1])
    rst_clean_avg.append([sum, cnt, sum/cnt, i[1]])


def tofile(outputFile, outputList):
  output = open(outputFile, 'w')
  for item in outputList:
    output.write("%s\n" % item)
  output.close()

qry = 'http://api.semrush.com/?type=phrase_related&key=apikey&display_limit=10&export_columns=Ph,Nq,Cp,Co,Nr&database=us&phrase='
kw = getQuery(dict,geoline)
outputList = runQuery(qry,kw)
tofile('semRushResultNew.csv', rst_clean)

---- end of related keyword experiment

---- specific search
dict_pa = {'Bankruptcy & Debt': ['bankruptcy'],
'Car Accidents':['car+accidents'],
'Child Custody':['joint+custody','child +custody'],
'Criminal Defense':['criminal+defense'],
'DUI & DWI':['dui','dwi'],
'Divorce & Separation':['divorce'],
'Domestic Violence':['domestic+violence'],
'Employment & Labor':['eeoc','employment'],
'Family':['family'],
'Immigration':['immigration'],
'Medical Malpractice':['medical+malpractice'],
'Personal Injury':['personal+injury'],
'Probate':['probate+will','probate'],
'Speeding & Traffic Ticket':['traffic+ticket','speeding+ticket'],
'Workers Compensation':['workers+compensation']}

list_geo = ['harris', 'seattle', 'los+angeles']
def getQuery(dict_pa, list_geo):
    kw = []
    for i in range(len(list_geo)):
      geo =  list_geo[i]
      for key in dict_pa:
          for values in dict_pa[key]:
            kw.append([geo, key, values+'+'+'attorney'+'+'+geo])
            kw.append([geo, key, values+'+'+'lawyer'+'+'+geo])
            kw.append([geo, key, values+'+'+'lawyers'+'+'+geo])
            kw.append([geo, key, values+'+'+'attorney'+'+in+'+geo])
            kw.append([geo, key, values+'+'+'lawyer'+'+in+'+geo])
            kw.append([geo, key, values+'+'+'lawyers'+'+in+'+geo])
            kw.append([geo, key, geo+'+'+values+'+'+'attorney'])
            kw.append([geo, key, geo+'+'+values+'+'+'lawyer'])
            kw.append([geo, key, geo+'+'+values+'+'+'lawyers'])
    return kw
kw = getQuery(dict_pa, list_geo)

def runQuery(qry, kw):
    output = []
    for i in kw:
        query = qry + i[2]
        try:
          print i
          resp = urllib.urlopen(query).read()
          semRushRst = resp.split('\r\n')[1:]
          output.append([semRushRst,i])
        except:
          print 'bad'
          print i
          pass
    return output

rst = runQuery(qry, kw)

rst_clean=[]
for i in rst:
   if len(i[0])>0:
     rst_clean.append(i)

rst_clean=[]
for i in rst:
  if len(i[0])>0:
    for k in i[0]:
      line = k.split(';')
      if line[2]>0:
        rst_clean.append([line[0],line[1],line[2], i[1]])

rst_clean_avg=[]
for i in rst:
  if len(i[0])>0:
    sum = 0
    cnt = 0
    for k in i[0]:
      line = k.split(';')
      if line[2]>0:
        sum += int(line[1]) * float(line[2])
        cnt += int(line[1])
    rst_clean_avg.append([sum, cnt, sum/cnt, i[1]])


def tofile(outputFile, outputList):
  output = open(outputFile, 'w')
  for item in outputList:
    output.write("%s\n" % item)
  output.close()

qry = 'http://api.semrush.com/?type=phrase_this&key=apikey&export_columns=Ph,Nq,Cp,Co,Nr&database=us&phrase='
kw = getQuery(dict,geoline)
outputList = runQuery(qry,kw)
tofile('semRushResultNew.csv', rst_clean)
---- end of specific search
