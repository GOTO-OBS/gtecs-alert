Instructions for running and using this sytem

1.
Move all the contents of this download to a user directory

2.
Run the set_up.py script to create relevant directories. it will create a directory called www with sub directories in it
all of these need to have permissions to be read from public

3.
Move index2.html, recent_ten.html and template.html into each of the goto_south/north_transient directories and then remove
the 2 from index2.html. In these files check that the html paths are set correctly. Make any changes you wish.

4.
check that all the python packages that are needed are installed (this is for python3.6). You will also need to have
https://comet.readthedocs.io/en/stable/ installed.

5.
in alert.py you will see this..

#used for local testing
with open("ivo__nasa.gsfc.gcn_SWIFTBAT_GRB_Pos_813449030", "rb") as f:
    v = vp.load(f)

#use this when live parsing is working
#v = vp.loads(sys.stdin.buffer.read())

if you wish to test locally with xml files leave as is. If you want to connect to comet you will need to make these adjustments

#used for local testing
#with open("ivo__nasa.gsfc.gcn_SWIFTBAT_GRB_Pos_813449030", "rb") as f:
#    v = vp.load(f)

#use this when live parsing is working
v = vp.loads(sys.stdin.buffer.read())

6.
note that the email and slack message functions have been commented out. this is to prevent spam with multiple
people testing. 

7.
run the system live with this terminal command. (it will probably need some adjusting)

/opt/local/bin/python3.6 /opt/local/bin/twistd --pidfile=/home/obrads/logs/twistd-comet.pid --umask=0002
--logfile=/home/obrads/logs/4pisky.log comet --local-ivo=ivo://org.goto-observatory/hwo
--remote=voevent.4pisky.org:8099 --cmd=/home/obrads/script/runscript.bash --eventdb=/home/obrads/.cometdb --verbose
