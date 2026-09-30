"""Render the documented logical architecture (requires Pillow)."""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import math

ROOT = Path(__file__).resolve().parents[1]
W,H=2200,1510
im=Image.new('RGB',(W,H),'#f3f6fa');d=ImageDraw.Draw(im)
fontroot=Path('/System/Library/Fonts/Supplemental')
def font(size,bold=False):
    name='Arial Bold.ttf' if bold else 'Arial.ttf'
    for path in [fontroot/name,Path('/usr/share/fonts/truetype/dejavu')/('DejaVuSans-Bold.ttf' if bold else 'DejaVuSans.ttf')]:
        if path.exists(): return ImageFont.truetype(str(path),size)
    return ImageFont.load_default(size=size)
def txt(x,y,text,size=27,color='#233650',bold=False):
    d.text((x,y),text,font=font(size,bold),fill=color)
def card(x,y,title,tag,lines,color,w=460,h=260):
    d.rounded_rectangle((x,y,x+w,y+h),radius=18,fill='white',outline='#d8e1eb',width=2)
    d.rounded_rectangle((x+20,y+20,x+w-20,y+55),radius=8,fill=color)
    txt(x+32,y+24,tag,20,'white',True)
    txt(x+24,y+76,title,32,bold=True)
    for i,line in enumerate(lines): txt(x+24,y+128+i*32,line,24)
def arrow(points,label=None,at=None,color='#3268a2',both=False):
    d.line(points,fill=color,width=5,joint='curve')
    def tip(a,b):
        theta=math.atan2(b[1]-a[1],b[0]-a[0]);r=16
        d.polygon([b,(b[0]-r*math.cos(theta-.5),b[1]-r*math.sin(theta-.5)),(b[0]-r*math.cos(theta+.5),b[1]-r*math.sin(theta+.5))],fill=color)
    tip(points[-2],points[-1])
    if both: tip(points[1],points[0])
    if label and at:
        f=font(22,True);b=d.textbbox(at,label,font=f)
        d.rounded_rectangle((b[0]-8,b[1]-6,b[2]+8,b[3]+6),radius=5,fill='#f3f6fa')
        d.text(at,label,font=f,fill=color)

d.rectangle((0,0,W,184),fill='#11243c')
txt(60,38,'EMPLOYEE SEARCH  /  ARCHITECTURE',48,'white',True)
txt(60,108,'MongoDB is authoritative. Kafka delivers committed events. OpenSearch serves an eventual search projection.',27,'#bed0e5')
txt(60,220,'WRITE & DELIVERY PATH',25,'#4f657f',True)
card(60,290,'Employee write service','SPRING BOOT 4  |  APP',[
    'POST /api/employees or /api/seed','API writes + background loader','500 employees per seed transaction','Java 21 / port 8080'], '#286caa')
card(600,290,'MongoDB transaction','MONGODB 8  |  SOURCE OF TRUTH',[
    'employees + outbox events','+ jobs checkpoint for seed batches','Single replica set rs0; majority writes','COMMIT all, or ROLLBACK all'], '#23775c')
card(1140,290,'Change-stream relay','SPRING BOOT 4  |  APP',[
    'Watch committed outbox inserts','Batch up to 500; resume from token','Kafka ACKs, then Mongo transaction:','delivery markers + resume token'], '#286caa')
card(1680,290,'employees.changed','KAFKA 3.9  |  PERSISTENT LOG',[
    '3 partitions / replication factor 1','Producer idempotence + acks=all','Employee ID is the partition key','KRaft broker + controller'], '#7954a0')
arrow([(520,407),(600,407)],'1', (550,365))
arrow([(1060,407),(1140,407)],'2: CDC',(1067,365))
arrow([(1600,407),(1680,407)],'3',(1630,365))
arrow([(1910,550),(1910,930)],'4  Consume',(1930,720))

# The three cards explain distinct failure boundaries without adding crossing arrows.
notes=[('ATOMIC WRITE','A failed MongoDB batch leaves no','partial employees, events or checkpoint.'),
       ('DURABLE RESUME','Restart from saved token. History lost?','Recover retained unpublished events.'),
       ('FAILURE HANDLING','Bulk failure: retry every 2 seconds.','Offsets advance only after success.')]
for i,(title,a,b) in enumerate(notes):
    x=60+i*530
    d.rounded_rectangle((x,650,x+500,826),radius=14,fill='#e8eef6')
    txt(x+20,674,title,23,bold=True)
    txt(x+20,720,a,23);txt(x+20,753,b,23)

card(60,930,'Browser / API client','USER ACCESS  |  LOCALHOST:8080',[
    'Generate at least 1M employees','Create and inspect nested JSON','Search, filter, paginate','Monitor Mongo / index / outbox counts'], '#435671',h=265)
card(600,930,'Search API','SPRING BOOT 4  |  APP',[
    'GET /api/search','Full text + exact filters','Nested skills and assignments','First 10,000 matches are pageable'], '#286caa',h=265)
card(1140,930,'employees-v1','OPENSEARCH 3.6  |  SEARCH INDEX',[
    'Bulk index; _id = employee ID','1 primary shard / 0 replicas','Nested skills + assignments','Search visibility refresh: 5 seconds'], '#bd7133',h=265)
card(1680,930,'Batch indexer','SPRING BOOT 4  |  APP',[
    'Consumer group: employee-indexer','3 consumers; up to 500 per poll','Check every bulk response for errors','Commit Kafka offsets after success'], '#286caa',h=265)
arrow([(1680,1050),(1600,1050)],'5',(1630,1008))
arrow([(1060,1050),(1140,1050)],'6',(1090,1008),both=True)
arrow([(520,1050),(600,1050)],'7',(550,1008),both=True)
# Separate write entry routing along the outer edge of the logical flow.
arrow([(60,1050),(25,1050),(25,420),(60,420)])
txt(60,865,'SEARCH PATH  /  COUNTS CAN LAG WHILE EVENTS ARE IN FLIGHT',24,'#4f657f',True)

d.rounded_rectangle((60,1250,2140,1440),radius=18,fill='#11243c')
txt(88,1277,'DEPLOYMENT & OPERATING BOUNDARIES',26,'white',True)
txt(88,1322,'4 Docker services: app, MongoDB, Kafka, OpenSearch. Mongo initialization is a one-shot helper.',25,'#d2dfed')
txt(88,1359,'Persistent volumes: mongo-data / kafka-data / opensearch-data. Host ports bind to 127.0.0.1.',25,'#d2dfed')
txt(88,1396,'One app instance. API-created records only; direct Mongo edits bypass the outbox. Single-node local demo.',25,'#d2dfed')
(ROOT/'docs'/'architecture.png').parent.mkdir(exist_ok=True)
im.save(ROOT/'docs'/'architecture.png',optimize=True)
print(ROOT/'docs'/'architecture.png')
