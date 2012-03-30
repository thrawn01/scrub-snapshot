
#dmsetup suspend "/dev/volume/original"
#dmsetup suspend "/dev/volume/backup"
#dmsetup resume "/dev/volume/original"
#dmsetup resume "/dev/volume/backup"

#DEVICE="/dev/mapper/lunr--volume-backup-cow"
#DEVICE="/dev/mapper/lunr--volume-backup"
#DEVICE="/dev/mapper/lunr--volume-thrawn"
#dmsetup remove $DEVICE
#exit 1

#dmsetup suspend $DEVICE
#dmsetup load $DEVICE --table "0 204800 error 8:16 18560"
#dmsetup resume $DEVICE

#dmsetup info $DEVICE
#dmsetup table $DEVICE
#dmsetup info "/dev/mapper/lunr--volume-backup"
#dmsetup table "/dev/mapper/lunr--volume-backup"
#dmsetup info "/dev/mapper/lunr--volume-thrawn"
#dmsetup table "/dev/mapper/lunr--volume-thrawn"
#dmsetup info "/dev/mapper/lunr--volume-thrawn-real"
#dmsetup table "/dev/mapper/lunr--volume-thrawn-real"
