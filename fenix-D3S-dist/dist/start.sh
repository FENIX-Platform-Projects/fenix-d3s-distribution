JAVA_OPTS="-server -Xmx2g -Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8 -Drhino.opt.level=9"
ORIENTDB_SETTINGS="-Dprofiler.enabled=true -Dorientdb.www.path="database/www""
LOG_SETTINGS="-Dlog4j.configuration="file:config/logging/log4j.properties""

java $JAVA_OPTS $ORIENTDB_SETTINGS $LOG_SETTINGS -jar lib/d3s.jar &