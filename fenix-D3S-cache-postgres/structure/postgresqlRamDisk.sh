mkdir -p /tmp/ramdisk
chmod 777 /tmp/ramdisk
mount -t tmpfs -o size=1G tmpfs /tmp/ramdisk/

mkdir /tmp/ramdisk/postgres
chmod 777 /tmp/ramdisk/postgres
chown postgres:postgres /tmp/ramdisk/postgres
