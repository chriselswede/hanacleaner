#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import sys, os, time, subprocess, re
from difflib import Differ
import signal

def printHelp():
    print("                                                                                                                                   ")    
    print("DESCRIPTION:                                                                                                                       ")
    print(" The HANA cleaner is a house keeping service for SAP HANA. It can be used to clean the backup catalog, diagnostic files,           ")
    print(" and alerts and to compress the backup logs. It should be executed by <sid>adm or, in case you use a CRON job, with the same       ")
    print(" environment as the <sid>adm. See SAP Note 2399996 and SAP Note 2400024.                                                           ")
    print("                                                                                                                                   ")
    print("INPUT ARGUMENTS:                                                                                                                   ")
    print("         ----  BACKUP ENTRIES in BACKUP CATALOG (and possibly BACKUPS)  ----                                                       ")
    print("         Note: In case you use Azure Backup you might want to check out this first                                                 ")
    print("         https://answers.sap.com/questions/13241352/manage-or-clean-up-the-hana-catalog-for-a-database.html                        ")
    print(" -be     minimum retained number of data backup (i.e. complete data backups and data snapshots) entries in the catalog, this       ") 
    print("         number of entries of data backups will remain in the backup catalog, all older log backup entries will also be removed    ")
    print("         with BACKUP CATALOG DELETE BACKUP_ID <id> (see SQL reference for more info) default: -1 (not used)                        ")
    print(" -bd     min retained days of data backup (i.e. complete data backups and data snapshots) entries in the catalog [days], the       ")
    print("         youngest successful data backup entry in the backup catalog that is older than this number of days is the oldest          ")              
    print("         successful data backup entry not removed from the backup catalog, default -1 (not used)                                   ")
    print("         Note: if both -be and -bd is used, the most conservative, i.e. the flag that removes the least number entries, decides    ")
    print("         Note: As mentioned in SAP Note 1812057 backup entries made via backint cannot be recovered, i.e. use -be and -bd with care")
    print("         if you want to be able to recover from older data backups (it is possible to recover from a specific data backup without  ")
    print("         the backup catalog)                                                                                                       ")
    print(" -bb     delete backups also [true/false], backups are deleted when the related backup catalog entries are deleted with            ")
    print("         BACKUP CATALOG DELETE BACKUP_ID <id> COMPLETE (see SQL reference for more info), default: false                           ")
    print(" -bo     output catalog [true/false], displays backup catalog before and after the cleanup, default: false                         ")
    print(" -br     output removed catalog entries [true/false], displays backup catalog entries that were removed, default: false            ")
    print("         Note: Please do not use -bo and -br if your catalog is huge (>10000) entries.                                             ")
    print(" -bn     output number deleted log backup entries [true/false], prints out how many log backup entries were deleted from the       ")
    print("         backup catalog, it is only needed to change this to false in case of extremely huge backup catalogs,   default: true      ")
    print("         ----  TRACE FILES  ----                                                                                                   ")
    print(" -tc     retention days for trace files [days], trace files with their latest modification time older than these number of days are")  #internal incident 1870190781
    print("         removed from all hosts, default: -1 (not used)                                                                            ")
    print("         Note: Conceptual -tc is the same as -tf, but -tc is using ALTER SYSTEM CLEAR TRACES ... See SQL Ref. for more info.       ")
    print("         Note: there is a bug (fixed with rev.122.11) that could cause younger trace files to be removed.                          ")
    print("         Note: expensive statements are not included in -tc, see -te below                                                         ")
    print(" -te     retention days for expensive statement files [days], same as for -tc, but only for expensive statement files, only use    ")  #internal incident 1980358670  
    print("         if you read and understood SAP Note 2819941 (should probably only be used with use_in_memory_tracking = false),           ")  #My opinion: this is a BUG!!
    print("         default: -1 (not used)                                                                                                    ")
    print(" -tcb    with backup [true/false], the trace files that are removed with the -tc and -te flags are backed up (as a .gz),           ")
    print("         i.e. ALTER SYSTEM CLEAR TRACES ... WITH BACKUP, see SQL Ref., default: false                                              ")
    print("         NOTE: Some small, unnecessary files, like .stat (SAP Note 2370780), might be deleted by WITH BACKUP --> the numbers might ")
    print("         not seem to fit                                                                                                           ") #internal incident 2180309626
    print(" -tbd    directory for the trace backups, full path of the directory to where the back up (see -tcb) of the trace files            ")
    print("         are moved (for housekeeping of moved files, see ANY FILES below),     default: '' (they stay in the original directory)   ")
    print(" -tmo    time out for move [seconds], the move, requested by the -tbd flag, must wait until the compression, requested by the -tcb ")
    print("         flag, is finished. This can take some seconds. If it is not finished before the time out, specified by the -tmo flag,     ")
    print("         the move will not happen, default: 30 seconds                                                                             ")
    print(" -tf     retention days for trace files [days], trace files, in all hosts, that are older than this number of days are removed     ")
    print("         (except for the currently opened trace files), only files with certain extensions like .trc, .log etc are taken into      ")
    print("         account, backup.log and backint.log, are excepted, please see -zb and -zp instead, default: -1 (not used)                 ")
    print("         Note: Conceptual -tf is the same as -tc, but -tf is using ALTER SYSTEM REMOVE TRACES ... See SQL Ref. for more info.      ")
    print("         Note: ALTER SYSTEM REMOVE TRACES ... WITH BACKUP does not exist, see SQL Ref., so there is no -tfb flag.                  ")
    print(" -to     output traces [true/false], displays trace files before and after the cleanup, default: false                             ")
    print(" -td     output deleted traces [true/false], displays trace files that were deleted, default: false                                ")
    print("         ----  DUMP FILES  ----                                                                                                    ")
    print(" -dr     retention days for dump files [days], manually created dump files (a.k.a. fullysytem dumps and runtime dumps) that are    ")
    print("         older than this number of days are removed, default: -1 (not used)                                                        ")
    print("         ----  ANY FILES  ----                                                                                                     ")
    print(" -gr     retention days for any general file [days], files in the directory specified with -gd and with the file names including   ")
    print("         the word specified with -gw are only saved for this number of days, default: -1 (not used)                                ")
    print("         Note: -gd and -gw can also be same length lists with a commas as delimiter                                                ")
    print(" -gd     directories for general files to be deleted, a comma separated list with full paths of directories with files to be       ")
    print('         deleted according to -gr (entries pairs with entries in -gw), default "" (not used)                                       ')
    print("         Note: if you include %SID, it will automatically be replaced with the actually SID of your system                         ")
    print(" -gw     filename parts for general files to be deleted, a comma separated list with words that files should have in their names   ")
    print('         to be deleted according to -gr (entries pairs with entries in -gd), default "" (not used)                                 ')
    print(" -gm     max depth, maximum recursive folders from folder specified by -gd it will delete files from, default: 1                   ")
    print("         ----  BACKUP LOGS <H2SPS04 ----                                                                                           ")
    print(" -zb     backup logs compression size limit [mb], if there are any backup.log or backint.log file (see -zp below) that is bigger   ")
    print("         than this size limit, then it is compressed and renamed, default: -1 (not used)                                           ")
    print("         Note: if -tf flag is used the resulting zip file could be removed by it.                                                  ")
    print("         Note: Don't use this with version HANA 2 SPS04 or later, instead configure size with parameters, see SAP Note 2797078     ")
    print(" -zp     zip path, specifies the path (and all subdirectories) where to look for the backup.log and backint.log files,             ")
    print("         default is the directory specified by the alias cdtrace                                                                   ")
    print(" -zl     zip links [true/false], specifies if symbolic links should be followed searching for backup logs in subdirectories        ")
    print("         of the directory defined by zp (or by alias cdtrace), default: false                                                      ")
    print(" -zo     print zipped backup logs, display the backup.log and backint.log that were zipped, default: false                         ")
    print(" -zk     keep zip, if this is set to false the zip file is deleted (use with care!), default: true                                 ")
    print("         ----  ALERTS  ----                                                                                                        ")
    print(" -ar     min retained alerts days [days], min age (today not included) of retained statistics server alerts, default: -1 (not used)")
    print(" -ao     output alerts [true/false], displays statistics server alerts before and after the cleanup, default: false                ")
    print(" -ad     output deleted alerts [true/false], displays statistics server alerts that were deleted, default: false                   ")
    print("         ----  OBJECT LOCKS ENTRIES with UNKOWN OBJECT NAME  ----                                                                  ")
    print(" -kr     min retained unknown object lock days [days], min age (today not included) of retained object lock entries with unknown   ")
    print("         object name, in accordance with SAP Note 2147247, default: -1 (not used)                                                  ")
    print("         ----  OBJECT HISTORY  ----                                                                                                ")
    print(" -om     object history table max size [mb], if the table _SYS_REPO.OBJECT_HISTORY is bigger than this threshold this table        ")
    print("         will be cleaned up according to SAP Note 2479702, default: -1 (not used)                                                  ")
    print(" -oo     output cleaned memory from object table [true/false], displays how much memory was cleaned up from object history         ")
    print("         table, default: false                                                                                                     ")
    print("         ---- LOG SEGMENTS  ----                                                                                                   ")
    print(" -lr     max free logsegments per service [number logsegments], if more free logsegments exist for a service the statement         ")
    print("         ALTER SYSTEM RECLAIM LOG is executed, default: -1 (not used)                                                              ")
    print("         ---- EVENTS  ----                                                                                                         ")
    print(" -eh     min retained days for handled events [day], minimum retained days for the handled events, handled events that are older   ")
    print("         are removed by first being acknowledged and then deleted, this is done for all hosts, default: -1 (not used)              ")
    print("         Note: Due to a current issue in HANA all events of type INFO are ignored. If automatic cleaning of INFO events are        ")
    print("               needed, please open an incident on component HAN-DB about the SQL statement ALTER SYSTEM SET EVENT HANDLED.         ")
    print(" -eu     min retained days for unhandled events [day], minimum retained days for events, events that are older are removed by      ")
    print("         first being handled and acknowledged and then deleted, this is done for all hosts, default: -1 (not used)                 ")
    print("         ----  AUDIT LOG  ----                                                                                                     ")
    print(" -ur     retention days for audit log table [days], audit log content older than these number of days is removed,                  ")
    print("         default: -1 (not used)                                                                                                    ")
    print("         ----  PENDING EMAILS  ----                                                                                                ")
    print(" -pe     retention days for pending e-mails [days], pending statistics server e-mail notifications older than these number of      ")
    print("         days are removed, default: -1 (not used)            (requires SELECT and DELETE on the _SYS_STATISTICS schema)            ")
    print("         ----  DATA VOLUMES FRAGMENTATION  ----                                                                                    ")
    print(" -fl     fragmentation limit [%], maximum fragmentation of data volume files, of any service, before defragmentation of that       ")
    print("         service is started: ALTER SYSTEM RECLAIM DATAVOLUME '<host>:<port>’ 120 DEFRAGMENT,        default: -1 (not used)         ")
    print("         Note: If you use System Replication see Q19 in SAP Note 1999880.                                                          ")
    print(" -fo     output fragmentation [true/false], displays data volume statistics before and after defragmentation, default: false       ")
    print("         ----  MULTIPLE ROW STORE TABLE CONTAINERS   ----                                                                          ")
    print(" -rc     row store containers cleanup [true/false], switch to clean up multiple row store table containers, default: false         ")
    print("         Note: Unfortunately there is NO nice way to give privileges to the DB User to be allowed to do this. Either you can       ")
    print("         run hanacleaner as SYSTEM user (NOT recommended) or grant DATA ADMIN to the user (NOT recommended)                        ")
    print(" -ro     output row containers [true/false], displays row store tables with more than one container before cleanup, default: false ")
    print("         ---- COMPRESSION OPTIMIZATION ----                                                                                        ")
    print("         1. Both following two flags, -cc, and -ce, must be > 0 to control the force compression optimization on tables that never ")
    print("         was compression re-optimized (i.e. last_compressed_record_count = 0):                                                     ")
    print(" -cc     max allowed raw main records, if table has more raw main rows --> compress if -ce, default: -1 (not used) e.g. 10000000   ")
    print(" -ce     max allowed estimated size [GB], if estimated size is larger --> compress if -cc, default: -1 (not used) e.g. 1           ") 
    print("         2. All following three flags, -cr, -cs, and -cd, must be > 0 to control the force compression optimization on tables with ")
    print("         columns with compression type 'DEFAULT' (i.e. no additional compression algorithm in main)                                ")
    print(" -cr     max allowed rows, if a column has more rows --> compress if -cs&-cd, default: -1 (not used) e.g. 10000000                 ")
    print(" -cs     max allowed size [MB], if a column is larger --> compress if -cr&-cd, default: -1 (not used) e.g. 500                     ")
    print(" -cd     min allowed distinct count [%], if a column has less distinct quota --> compress if -cr&-cs, default -1 (not used) e.g. 5 ") 
    print("         3. Both following two flags, -cu and -cq, must be > 0 to control the force compression optimization on tables whose UDIV  ")
    print("         quota is too large, i.e. #UDIVs/(#raw main + #raw delta)                                                                  ")
    print(" -cq     max allowed UDIV quota [%], if the table has larger UDIV quota --> compress if -cu, default: -1 (not used) e.g. 150       ")
    print(" -cu     max allowed UDIVs, if a column has more then this number UDIVs --> compress if -cq, default: -1 (not used) e.g. 10000000  ")
    print("         4. Flag -cb must be > 0 to control the force compression optimization on tables with columns with SPARSE (<122.02) or     ")
    print("         PREFIXED and a BLOCK index                                                                                                ")
    print(" -cb     max allowed rows, if a column has more rows and a BLOCK index and SPARSE (<122.02) or PREFIXED then this table should     ")
    print("         be compression re-optimized, default -1 (not used) e.g. 100000                                                            ")
    print("         Following three flags are general; they control all three, 1., 2., 3., 4., compression optimization possibilities above   ")
    print(" -cp     per partition [true/false], switch to consider flags above per partition instead of per column, default: false            ")
    print(" -cm     merge before compress [true/false], switch to perform a delta merge on the tables before compression, default: false      ")
    print(" -co     output compressed tables [true/false], switch to print all tables that were compression re-optimized, default: false      ")
    print("         ---- VIRTUAL TABLE STATISTICS CREATION ----                                                                               ")
    print(" -vs     create statistics for virtual tables [true/false], switch to create optimization statistics for those virtual tables      ")
    print("         that are missing statistics according to SAP Note 1872652 (Note: could cause expenive operations),    default: false      ")
    print(" -vt     default statistics type [HISTOGRAM/SIMPLE/TOPK/SKETCH/SAMPLE/RECORD_COUNT], type of data statistics object                ")
    print("         default: HISTOGRAM                                                                                                        ")
    print(" -vn     max number of rows for defult type [number rows], if the VT has less or equal number of rows specified by -vn the default ")
    print("         statistics type, defined by -vt, is used, else the type defined by -vtt is used,           default: -1 (not considered)   ")
    print(" -vtt    large statistics type [HISTOGRAM/SIMPLE/TOPK/SKETCH/SAMPLE/RECORD_COUNT], type of data statistics object used if the VT   ")
    print("         has more rows than specified by -vn and the database is HANA                                    default: SIMPLE           ")
    print(" -vto    statistics type for other DBs [HISTOGRAM/SIMPLE/TOPK/SKETCH/SAMPLE/RECORD_COUNT], type of data statistics object if the   ")
    print("         remote database is not HANA                                                                 default: "" (not considered)  ") 
    print(" -vl     schema list of virtual tables, if you only want tables in some schemas to be considered for the creation of statistics    ")
    print("         provide here a comma separated list of those schemas, default '' (all schemas will be considered)                         ")
    print(" -vr     ignore secondary monitoring tables [true/false], normaly statistics for the the virtual tables in the                     ")
    print("         _SYS_SR_SITE* schemas are not needed to be created nor updated, so they are by default ignored, default: true             ")
    print("         ---- VIRTUAL TABLE STATISTICS REFRESH ----                                                                                ")
    print(" -vnr    refresh age of VT statistics [number days > 0], if the VT statistics of a table is older than this number of days it will ")
    print("         be refreshed      (Note: -vl and -vr holds also for refresh)                                   default: -1 (no refresh)   ")
    print("         ---- INIFILE CONTENT HISTORY ----                                                                                         ")
    print(" -ir     inifile content history retention [days], deletes older inifile content history, default: -1 (not used) (should > 1 year) ")
    print("         Note: Only supported with 03<SPS<SPS05.                                                                                   ")
    print("         ---- INTERVALL  ----                                                                                                      ")
    print(" -hci    hana cleaner interval [days], number days that hanacleaner waits before it restarts, default: -1 (exits after 1 cycle)    ")
    print("         NOTE: Do NOT use if you run hanacleaner in a cron job!                                                                    ")
    print("         ---- INPUT  ----                                                                                                          ")
    print(" -ff     flag file(s), a comma seperated list of full paths to files that contain input flags, each flag in a new line, all lines  ")
    print("         in the files that do not start with a flag (a minus) are considered comments, default: '' (not used)                      ")
    print("         Note: if you include %SID in the path, it will automatically be replaced with the actually SID of your system             ")
    print("         ---- EXECUTE  ----                                                                                                        ")
    print(" -es     execute sql [true/false], execute all crucial housekeeping tasks (useful to turn off for investigation with -os=true,     ")
    print("         a.k.a. chicken mode :)  default: true                                                                                     ")
    print("         ---- OUTPUT  ----                                                                                                         ")
    print(" -os     output sql [true/false], prints all crucial housekeeping tasks (useful for debugging with -es=false), default: false      ")
    print(" -op     output path, full literal path of the folder for the output logs (will be created if not there), default = '' (not used)  ")
    print("         Note: if you include %SID in the output path, it will automatically be replaced with the actually SID of your system      ")
    print(" -of     output prefix, adds a string to the output file, default: ''   (not used)                                                 ")
    print(" -or     output retention days, logs in the paths specified with -op are only saved for this number of days, default: -1 (not used)")
    print(" -oc     output configuration [true/false], logs all parameters set by the flags and where the flags were set, i.e. what flag file ")
    print("         (one of the files listed in -ff) or if it was set via a flag specified on the command line, default = false               ")
    print(" -so     standard out switch [true/false], switch to write to standard out, default:  true                                         ")
    print("         ---- INSTANCE ONLINE CHECK ----                                                                                           ")
    print(" -oi     online test interval [seconds], < 0: HANACleaner does not check if online or secondary,           default: -1 (not used)  ")
    print("                                         = 0: if not online or not primary HANACleaner will abort                                  ")
    print("                                         > 0: time it waits before it checks if DB is online and primary again                     ")
    print("                                              Note: For the > 0 option it might be necessary to use cron with the lock option      ")
    print("                                                    See the HANASitter & CRON slide in the HANASitter pdf                          ")
    print("         Note: for now, -oi is not supported for a server running HANA Cockpit                                                     ")
    print("         ---- SERVER FULL CHECK ----                                                                                               ")
    print(" -fs     file system, path to server to check for disk full situation before hanacleaner runs, default: blank, i.e. df -h is used  ")
    print('                      Could also be used to specify a couple of servers with e.g. -fs "|grep sapmnt"                               ')
    print(" -if     ignore filesystems and mounts, before hanacleaner starts it checks that there is no disk full situation in any of the     ")
    print("         filesystems and/or mounts, this flag makes it possible to ignore some filesystems, with comma separated list, from the    ")
    print("         df -h  command (filesystems are in the first column and mounts normally in the 5th or 6th column), default: ''            ")
    print(" -df     filesystem check switch [true/false], it is possible to completely ignore the filesystem check (necessary if non-ascii    ")
    print("         comes out from  df -h). However, hanacleaner is NOT supported in case of full filesystem so if you turn this to false     ")
    print("         it is necessary that you check for disk full situation manually! default: true                                            ")
    print("         ----  SSL  ----                                                                                                           ")   
    print(" -ssl    turns on ssl certificate [true/false], makes it possible to use SAP HANA Cleaner despite SSL, default: false              ")
    print("         ----  HOST  ----                                                                                                          ")
    print(" -vlh    virtual local host, if hanacleaner runs on a virtual host this has to be specified, default: '' (physical host is assumed)")
    print("         ----  USER KEY  ----                                                                                                      ")     
    print(" -k      DB user key, this one has to be maintained in hdbuserstore, i.e. as <sid>adm do                                           ")               
    print("         > hdbuserstore SET <DB USER KEY> <ENV> <USERNAME> <PASSWORD>                     , default: SYSTEMKEY                     ")
    print("         It could also be a list of comma seperated userkeys (useful in MDC environments), e.g.: SYSTEMKEY,TENANT1KEY,TENANT2KEY   ")
    print("         Note: It is not possible to use underscore in the user key, e.g. HANA_HOUSEKEEPING is NOT possible                        ")
    print(" -dbs    DB key, this can be a list of databases accessed from the system defined by -k (-k can only be one key if -dbs is used)   ")               
    print("         Note: Users with same name and password have to be maintained in all databases   , default: ''  (not used)                ")
    print("         It is possible to specify  -dbs all  to execute hanacleaner on all active databases, then -k must point to SYSTEMDB       ")
    print("         Example:  -k PQLSYSDBKEY -dbs SYSTEMDB,PQL,PQ2                                                                            ")
    print("         Example:  -k PQLSYSDBKEY -dbs all                                                                                         ")
    print("         ---- EMAIL ----                                                                                                           ")
    print(" -en     email notification for most fatal errors, <receiver 1's email>,<receiver 2's email>,... default:          (not used)      ") 
    print(" -et     email timeout warning [seconds], sends email to the email addresses specified with -en, if HANACleaner took longer time   ")
    print("         than specified with -et, default: -1 (not used)                                                                           ")
    print(" -ena    always send email, if set to true, a summary of the hanacleaner run is always send (-en must be true), default: false     ")
    print(" -enc    email client, to explicitly specify the client (e.g mail, mailx, mutt,..), only useful if -en if used, default: mailx     ") 
    print(" -ens    sender's email, to explicitly specify sender's email address, only useful if -en if used, default:    (configured used)   ")
    print(" -enm    mail server, to explicitly specify the mail server, only useful if -en is used, default:     (configured used)            ")
    print('         NOTE: For this to work you have to install the linux program "sendmail" and add a line similar to                         ')
    print('               DSsmtp.intra.ourcompany.com in the file sendmail.cf in /etc/mail/,                                                  ')
    print("               see https://www.systutorials.com/5167/sending-email-using-mailx-in-linux-through-internal-smtp/                     ")
    print("                                                                                                                                   ")    
    print("                                                                                                                                   ")    
    print("EXAMPLE (trace files, statistics server alerts and backup catalog entries, i.e. not the backups themselves, older than 42 days     ")
    print("         are deleted and backup logs bigger than 50 mb are compressed and renamed and logsegments a removed if more than 20        ")
    print("         free once exist for a service):                                                                                           ")
    print("                                                                                                                                   ")
    print("  > python hanacleaner.py -tc 42 -tf 42 -ar 42 -bd 42 -zb 50 -lr 20 -eh 2 -eu 42                                                   ")
    print("                                                                                                                                   ")
    print("                                                                                                                                   ")
    print("EXAMPLE (reads a configuration file, one flag will overwrite, i.e. retention time for the alerts will be 200 days instead of 42):  ")
    print("  > python hanacleaner.py -ff /tmp/HANACleaner/hanacleaner_configfile.txt -ar 200                                                  ")
    print("    Where the config file could look like this:                                                                                    ")
    print("                                  MY HANACLEANER CONFIGURATION FILE                                                                ")
    print("                                  Oldest content of the trace files should only be 42 days old                                     ")
    print("                                  -tc 42                                                                                           ")
    print("                                  Oldest trace file should only be 42 days old                                                     ")
    print("                                  -tf 42                                                                                           ")
    print("                                  Oldest alerts should only be 42 days old                                                         ")
    print("                                  -ar 42                                                                                           ")
    print("                                  This is the key in hdbuserstore that is used:                                                    ")
    print("                                  -k SYSTEMKEY                                                                                     ")
    print("                                                                                                                                   ")
    print("CURRENT KNOWN LIMITATIONS (i.e. TODO LIST):                                                                                        ")
    print(" 1. HANACleaner should notice if HANA is not listening to SQL or only readable and then                                            ")
    print("    sleep for a while and test if this HANA becomes primary now and then --> useful in case of HSR                                 ")
    print(" 2. Allow granular control on minutes instead of days                                                                              ")
    print(" 3. Allow compression on trace files as well not only on backup related files                                                      ")
    print(" 4. Allow a two steps cleanup for general files, e.g. compress file older than a few hours and delete files older than some days   ")
    print(" 5. Check for multiple definitions of one flag, give ERROR, and STOP                                                               ")
    print(" 6. Move trace files instead of deleting ... --> not a good idea ... should not touch trace files from OS, only from HANA          ")
    print(" 7. Only send emails in case of some failure, either an found error or a catched error                                             ")
    print(" 8. HANA Cleaner should be able to clean up its own tracefiles (-or) even though it is Secondary on a System Replicaiton setup     ")
    print("    The same is true about some other cleanups... e.g. ANY FILES                                                                   ")
    print(" 9. It should be possible to influence the ignore list of the -tf flag with a flag getting a list of files not to delete           ")
    print("                                                                                                                                   ")
    print("AUTHOR: Christian Hansen                                                                                                           ")
    print("                                                                                                                                   ")
    print("                                                                                                                                   ")
    os._exit(1)
    
def printDisclaimer():
    print("                                                                                                                                   ")    
    print("ANY USAGE OF HANACLEANER ASSUMES THAT YOU HAVE UNDERSTOOD AND AGREED THAT:                                                         ")
    print(" 1. HANACleaner is NOT SAP official software, so normal SAP support of HANACleaner cannot be assumed                               ")
    print(" 2. HANACleaner is open source                                                                                                     ") 
    print(' 3. HANACleaner is provided "as is"                                                                                                ')
    print(' 4. HANACleaner is to be used on "your own risk"                                                                                   ')
    print(" 5. HANACleaner is a one-man's hobby (developed, maintained and supported only during non-working hours)                           ")
    print(" 6  All HANACleaner documentations have to be read and understood before any usage:                                                ")
    print("     a) SAP Note 2399996                                                                                                           ")
    print("     b) The .pdf file that can be downloaded from https://github.com/chriselswede/hanacleaner                                      ")
    print("     c) All output from executing                                                                                                  ")
    print("                     python hanacleaner.py --help                                                                                  ")
    print(" 7. HANACleaner can help you execute certain SAP HANA tasks automatically but is NOT an attempt to teach you SAP HANA              ")
    print("    Therefore it is assumed that you understand all SQL statements that HANACleaner does to make changes in your system            ")
    print("    To find out what crucial SQL statements HANACleaner will do without actually executing them run with the additional flags      ")
    print("            -es false -os true                                                                                                     ")
    print('    To then learn what those statements do before you executing HANACleaner without "-es false", see SAP HANA Admin Guide or       ')
    print("    SAP HANA System Administration Workshops                                                                                       ")
    print(" 8. HANACleaner is not providing any recommendations, all flags shown in the documentation (see point 6.) are only examples        ")
    print("    For recommendations see SAP HANA Administration Workshops or other documentation, like e.g. SAP Note 2400024                   ")
    os._exit(1)

######################## CLASS DEFINITIONS ################################

class SQLManager:
    def __init__(self, execute_sql, hdbsql_string, dbuserkey, DATABASE, log_sql):
        self.execute = execute_sql
        self.key = dbuserkey
        self.db = DATABASE
        self.log = log_sql
        if len(DATABASE) > 1:
            self.hdbsql_jAU = hdbsql_string + " -j -A -U " + self.key + " -d " + self.db
            self.hdbsql_jAxU = hdbsql_string + " -j -A -x -U " + self.key + " -d " + self.db
            self.hdbsql_jAaxU = hdbsql_string + " -j -A -a -x -U " + self.key + " -d " + self.db
            self.hdbsql_jAQaxU = hdbsql_string + " -j -A -Q -a -x -U " + self.key + " -d " + self.db
        else:
            self.hdbsql_jAU = hdbsql_string + " -j -A -U " + self.key
            self.hdbsql_jAxU = hdbsql_string + " -j -A -x -U " + self.key
            self.hdbsql_jAaxU = hdbsql_string + " -j -A -a -x -U " + self.key
            self.hdbsql_jAQaxU = hdbsql_string + " -j -A -Q -a -x -U " + self.key

class LogManager:
    def __init__(self, log_path, out_prefix, print_to_std, emailSender):
        self.path = log_path
        self.out_prefix = out_prefix
        if self.out_prefix:
            self.out_prefix = self.out_prefix + "_"
        self.print_to_std = print_to_std
        self.emailSender = emailSender

class EmailSender:
    def __init__(self, receiverEmails, emailClient, senderEmail, mailServer, SID):
        self.senderEmail = senderEmail
        self.emailClient = emailClient
        self.receiverEmails = receiverEmails
        self.mailServer = mailServer
        self.SID = SID
    def printEmailSender(self):
        print("Email Client: ", self.emailClient)
        if self.senderEmail:
            print("Sender Email: ", self.senderEmail)
        else:
            print("Configured sender email will be used.")
        if self.mailServer:
            print("Mail Server: ", self.mailServer)
        else:
            print("Configured mail server will be used.")
        print("Reciever Emails: ", self.recieverEmails)

######################## FUNCTION DEFINITIONS ################################

def run_command(cmd):
    if sys.version_info[0] == 2: 
        out = subprocess.check_output(cmd, shell=True).strip("\n")
    elif sys.version_info[0] == 3:
        out = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip("\n")
    else:
        print("ERROR: Wrong Python version")
        os._exit(1)
    return out

def get_sid():
    SID = run_command('echo $SAPSYSTEMNAME').upper()
    return SID

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def log(message, logmanager, send_email = False):
    if logmanager.print_to_std:
        print(message)
    if logmanager.path:
        file_name = "hanacleanerlog"
        logfile = open(logmanager.path+"/"+file_name+"_"+logmanager.out_prefix+datetime.now().strftime("%Y-%m-%d"+".txt").replace(" ", "_"), "a")
        logfile.write(message+"\n")   
        logfile.flush()
        logfile.close()
    if send_email and logmanager.emailSender:  #sends email IF this call of log() wants it AND IF -en flag has been specified with email(s)
        sendEmail(message, logmanager)

def sendEmail(message, logmanager):       
    message = 'Hi Team, \n\nHANACleaner reports:\n\n'+message
    mailstring = 'echo "'+message+'" | '+logmanager.emailSender.emailClient+' -s "Message from HANACleaner on '+logmanager.emailSender.SID+'" '
    if logmanager.emailSender.mailServer:
        mailstring += ' -S smtp=smtp://'+logmanager.emailSender.mailServer+' '
    if logmanager.emailSender.senderEmail:
        mailstring += ' -S from="'+logmanager.emailSender.senderEmail+'" '
    mailstring += ",".join(logmanager.emailSender.receiverEmails)
    #output = subprocess.check_output(mailstring, shell=True)
    dummyout = run_command(mailstring)

def try_execute_sql(sql, errorlog, sqlman, logman, exit_on_fail = True):
    succeeded = True
    out = ""
    try:
        if sqlman.log:
            log(sql, logman)
        if sqlman.execute:
            #out = subprocess.check_output(sqlman.hdbsql_jAaxU + " \""+sql+"\"", shell=True)
            out = run_command(sqlman.hdbsql_jAaxU + " \""+sql+"\"")
    except:
        errorMessage = "ERROR: Could not execute\n"+sql+"\n"+errorlog
        succeeded = False
        if exit_on_fail:
            log(errorMessage, logman, True)
            os._exit(1)
        else:
            log(errorMessage, logman)
    return [out, succeeded]

def is_email(s):
    s = s.split('@')
    if not len(s) == 2:
        return False
    return '.' in s[1]

def hana_version_revision_maintenancerevision(sqlman, logman):
    #command_run = subprocess.check_output(sqlman.hdbsql_jAU + " \"select value from sys.m_system_overview where name = 'Version'\"", shell=True)
    command_run = run_command(sqlman.hdbsql_jAU + " \"select value from sys.m_system_overview where name = 'Version'\"")
    hanaver = command_run.splitlines(1)[2].split('.')[0].replace('| ','')
    hanarev = command_run.splitlines(1)[2].split('.')[2]
    hanamrev = command_run.splitlines(1)[2].split('.')[3]
    if not is_integer(hanarev):
        log("ERROR: something went wrong checking hana revision.", logman, True)
        os._exit(1)
    return [int(hanaver), int(hanarev), int(hanamrev)]
    
def hosts(sqlman):
    #hosts = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"select distinct(host) from sys.m_host_information\"", shell=True).splitlines(1)
    hosts = run_command(sqlman.hdbsql_jAaxU + " \"select distinct(host) from sys.m_host_information\"").splitlines(1)
    hosts = [host.strip('\n').strip('|').strip(' ') for host in hosts]
    return hosts

def get_key_info(dbuserkey, local_host, logman):
    try:
        #key_environment = subprocess.check_output('''hdbuserstore LIST '''+dbuserkey, shell=True)
        key_environment = run_command('''hdbuserstore LIST '''+dbuserkey) 
    except:
        log("ERROR, the key "+dbuserkey+" is not maintained in hdbuserstore.", logman, True)
        os._exit(1)
    key_environment = key_environment.split('\n')
    key_environment = [ke for ke in key_environment if ke and not ke == 'Operation succeed.']
    ENV = key_environment[1].replace('  ENV : ','').replace(';',',').split(',')
    key_hosts = [env.split(':')[0].split('.')[0] for env in ENV]  #if full host name is specified in the Key, only the first part is used 
    DATABASE = ''
    if len(key_environment) == 4:   # if DATABASE is specified in the key, this will by used in the SQLManager (but if -dbs is specified, -dbs wins)
        DATABASE = key_environment[3].replace('  DATABASE: ','').replace(' ', '')
    if not local_host in key_hosts:
        print("ERROR, local host, ", local_host, ", should be one of the hosts specified for the key, ", dbuserkey, " (in case of virtual, please use -vlh, see --help for more info)")
        os._exit(1)
    return  [key_hosts, ENV, DATABASE]

def sql_for_backup_id_for_min_retained_days(minRetainedDays):
    oldestDayForKeepingBackup = datetime.now() + timedelta(days = -int(minRetainedDays))
    return "SELECT TOP 1 ENTRY_ID, SYS_START_TIME from sys.m_backup_catalog where (ENTRY_TYPE_NAME = 'complete data backup' or ENTRY_TYPE_NAME = 'data snapshot') and STATE_NAME = 'successful' and SYS_START_TIME < '" + oldestDayForKeepingBackup.strftime('%Y-%m-%d')+" 00:00:00' order by SYS_START_TIME desc"


def sql_for_backup_id_for_min_retained_backups(minRetainedBackups):
    return "SELECT ENTRY_ID, SYS_START_TIME from (SELECT ENTRY_ID, SYS_START_TIME, ROW_NUMBER() OVER(ORDER BY SYS_START_TIME desc) as NUM from sys.m_backup_catalog where (ENTRY_TYPE_NAME = 'complete data backup' or ENTRY_TYPE_NAME = 'data snapshot') and STATE_NAME = 'successful' order by SYS_START_TIME desc) as B where B.NUM = "+str(minRetainedBackups)

def online_and_master_tests(online_test_interval, local_dbinstance, local_host, logman):
    if online_test_interval < 0: #then dont test
        return True
    else:
        if is_online(local_dbinstance, logman) and not is_secondary(logman): 
            return is_master(local_dbinstance, local_host, logman)  #HANACleaner should only run on the Master Node
        else:
            return False

def is_master(local_dbinstance, local_host, logman):
    process = subprocess.Popen(['python', cdalias('cdpy', local_dbinstance)+"/landscapeHostConfiguration.py"], stdout=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode()
    out_lines = out.splitlines(1)
    host_line = [line for line in out_lines if local_host in line or local_host.upper() in line or local_host.lower() in line]  #have not tested this with virtual and -vlh yet
    if len(host_line) != 1:
        print_out = "ERROR: Something went wrong. It found more than one (or none) host line" + " \n ".join(host_line)
        log(print_out, logman, True)
        os._exit(1)
    nameserver_actual_role = host_line[0].strip('\n').split('|')[11].strip(' ')
    test_ok = (str(err) == "None")
    result = nameserver_actual_role == 'master'
    printout = "Master Check      , "+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"    ,     -            , "+str(test_ok)+"         , "+str(result)+"       , Nameserver actual role = "+nameserver_actual_role
    log(printout, logman)
    return result

def is_online(dbinstance, logman): #Checks if all services are GREEN and if there exists an indexserver (if not this is a Stand-By) 
    process = subprocess.Popen(['sapcontrol', '-nr', dbinstance, '-function', 'GetProcessList'], stdout=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode()
    number_services = out.count(" HDB ") + out.count(" Local Secure Store")   
    number_running_services = out.count("GREEN")
    number_indexservers = int(out.count("hdbindexserver")) # if not indexserver this is Stand-By
    test_ok = (str(err) == "None")
    result = (number_running_services == number_services) and (number_indexservers != 0)
    printout = "Online Check      , "+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"    ,     -            , "+str(test_ok)+"         , "+str(result)+"       , # index services: "+str(number_indexservers)+", # running services: "+str(number_running_services)+" out of "+str(number_services)
    log(printout, logman)
    return result
    
def is_secondary(logman):
    process = subprocess.Popen(['hdbnsutil', '-sr_state'], stdout=subprocess.PIPE)
    out, err = process.communicate() 
    out = out.decode()
    test_ok = (str(err) == "None")
    result = "active primary site" in out   # then it is secondary!
    printout = "Primary Check     , "+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"    ,     -            , "+str(test_ok)+"         , "+str(not result)+"       , " 
    log(printout, logman)
    return result 

def get_all_databases(execute_sql, hdbsql_string, dbuserkey, local_host, out_sql, logman): 
    all_db_out = ''
    [key_hosts, ENV, DATABASE] = get_key_info(dbuserkey, local_host, logman)
    key_sqlports = [env.split(':')[1] for env in ENV]     
    if not key_sqlports[0][-2:] == '13':
        log("ERROR: If  -bds all  is used, then  -k  must point to SYSTEMDB. Please see --help for more information.", logman)
        os._exit(1)
    sqlman = SQLManager(execute_sql, hdbsql_string, dbuserkey, DATABASE, out_sql)
    all_db_out = run_command(sqlman.hdbsql_jAaxU + " \"select DATABASE_NAME from M_DATABASES WHERE (ACTIVE_STATUS = 'YES')\"")
    all_databases = [line.strip('|').strip(' ') for line in all_db_out.splitlines()]
    if (len(all_databases) == 0) or \
        (len(all_databases) == 1) and all_databases[0] == '':
        log("\nERROR: No active database found. ", logman)
        os._exit(1)
    return all_databases

def checkIfAcceptedFlag(word):
    if not word in ["-h", "--help", "-d", "--disclaimer", "-ff", "-be", "-bd", "-bb", "-bo", "-br", "-bn", "-tc", "-te", "-tcb", "-tbd", "-tmo", "-tf", "-to", "-td", "-dr", "-gr", "-gd", "-gw", "-gm", "-zb", "-zp", "-zl", "-zo", "-zk", "-ar", "-kr", "-ao", "-ad", "-om", "-oo", "-lr", "-eh", "-eu", "-ur", "-pe", "-fl", "-fo", "-rc", "-ro", "-cc", "-ce", "-cr", "-cs", "-cd", "-cq", "-cu", "-cb", "-cp", "-cm", "-co", "-vs", "-vt", "-vn", "-vtt", "-vto", "-vr", "-vnr", "-vl", "-ir", "-es", "-os", "-op", "-of", "-or", "-oc", "-oi", "-fs", "-if", "-df", "-hci", "-so", "-ssl", "-vlh", "-k", "-dbs", "-en", "-et", "-ena", "-enc", "-ens", "-enm"]:
        print("INPUT ERROR: ", word, " is not one of the accepted input flags. Please see --help for more information.")
        os._exit(1)

def getParameterFromFile(flag, flag_string, flag_value, flag_file, flag_log, parameter):
    if flag == flag_string:
        parameter = flag_value
        flag_log[flag_string] = [flag_value, flag_file]
    return parameter

def getParameterListFromFile(flag, flag_string, flag_value, flag_file, flag_log, parameter):
    if flag == flag_string:
        parameter = [x for x in flag_value.split(',')]
        flag_log[flag_string] = [flag_value, flag_file]
    return parameter

def getParameterFromCommandLine(sysargv, flag_string, flag_log, parameter):
    if flag_string in sysargv:
        flag_value = sysargv[sysargv.index(flag_string) + 1]
        parameter = flag_value
        flag_log[flag_string] = [flag_value, "command line"]
    return parameter

def getParameterListFromCommandLine(sysargv, flag_string, flag_log, parameter):
    if flag_string in sysargv:
        parameter = [x for x in sysargv[  sysargv.index(flag_string) + 1   ].split(',')]
        flag_log[flag_string] = [','.join(parameter), "command line"]
    return parameter
   
def backup_id(minRetainedBackups, minRetainedDays, sqlman):
    if minRetainedDays >= 0:
        #results = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"" + sql_for_backup_id_for_min_retained_days(minRetainedDays) + "\"", shell=True).splitlines(1)
        results = run_command(sqlman.hdbsql_jAQaxU + " \"" + sql_for_backup_id_for_min_retained_days(minRetainedDays) + "\"").splitlines(1)
        [backupIdForMinRetainedDays, startTimeForMinRetainedDays] = results if results else ['', '']
        if not backupIdForMinRetainedDays:
            backupIdForMinRetainedDays = '-1'
            startTimeForMinRetainedDays = '1000-01-01 08:00:00'
        else:
            backupIdForMinRetainedDays = backupIdForMinRetainedDays.strip('\n').strip(' ')
            startTimeForMinRetainedDays = startTimeForMinRetainedDays.strip('\n').strip(' ').split('.')[0]  #removing milliseconds
    if minRetainedBackups >= 0:
        #results = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"" + sql_for_backup_id_for_min_retained_backups(minRetainedBackups) + "\"", shell=True).splitlines(1)
        results = run_command(sqlman.hdbsql_jAQaxU + " \"" + sql_for_backup_id_for_min_retained_backups(minRetainedBackups) + "\"").splitlines(1)
        [backupIdForMinRetainedBackups, startTimeForMinRetainedBackups] = results if results else ['', '']
        if not backupIdForMinRetainedBackups:
            backupIdForMinRetainedBackups = '-1'
            startTimeForMinRetainedBackups = '1000-01-01 08:00:00'
        else:
            backupIdForMinRetainedBackups = backupIdForMinRetainedBackups.strip('\n').strip(' ')
            startTimeForMinRetainedBackups = startTimeForMinRetainedBackups.strip('\n').strip(' ').split('.')[0]  #removing milliseconds
    if minRetainedDays >= 0 and minRetainedBackups >= 0:    
        backupId = backupIdForMinRetainedDays if datetime.strptime(startTimeForMinRetainedDays, '%Y-%m-%d %H:%M:%S')  < datetime.strptime(startTimeForMinRetainedBackups, '%Y-%m-%d %H:%M:%S') else backupIdForMinRetainedBackups
    elif minRetainedDays >= 0:
        backupId = backupIdForMinRetainedDays
    elif minRetainedBackups >= 0:
        backupId = backupIdForMinRetainedBackups
    else:
        backupId = ""
    return backupId

def sqls_for_backup_catalog_cleanup(minRetainedBackups, minRetainedDays, deleteBackups, sqlman):
    sqls = []
    backupId = backup_id(minRetainedBackups, minRetainedDays, sqlman)
    if backupId:
        #backupType = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select ENTRY_TYPE_NAME from sys.m_backup_catalog where backup_id = '"+backupId+"'\"", shell=True).strip('\n').strip(' ')
        backupType = run_command(sqlman.hdbsql_jAQaxU + " \"select ENTRY_TYPE_NAME from sys.m_backup_catalog where backup_id = '"+backupId+"'\"").strip(' ')
        if backupType == "complete data backup" or backupType == "data snapshot":
            sqls = ["BACKUP CATALOG DELETE ALL BEFORE BACKUP_ID " + backupId]
            if deleteBackups:
                sqls = ["BACKUP CATALOG DELETE ALL BEFORE BACKUP_ID " + backupId + " COMPLETE"]
        #If it will ever be possible to do    BACKUP CATALOG DELETE BACKUP_ID <log backup id>    then this will be useful:
        else:
            #backupIdStartTime = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select SYS_START_TIME from sys.m_backup_catalog where backup_id = '"+backupId+"'\"", shell=True).strip(' ')
            backupIdStartTime = run_command(sqlman.hdbsql_jAQaxU + " \"select SYS_START_TIME from sys.m_backup_catalog where backup_id = '"+backupId+"'\"").strip(' ')         
            #olderBackupIds = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select BACKUP_ID from sys.m_backup_catalog where SYS_START_TIME < '"+backupIdStartTime+"'\"", shell=True).splitlines()
            olderBackupIds = run_command(sqlman.hdbsql_jAQaxU + " \"select BACKUP_ID from sys.m_backup_catalog where SYS_START_TIME < '"+backupIdStartTime+"'\"").splitlines()
            olderBackupIds = [x.strip('\n').strip(' ') for x in olderBackupIds if x]
            for oldID in olderBackupIds:
                sql = "BACKUP CATALOG DELETE BACKUP_ID " + oldID
                if deleteBackups:
                    sql += " COMPLETE"
                sqls.append(sql)
    return sqls
        
def print_removed_entries(before, after, logman):
    beforeLines = before.splitlines(1) 
    beforeLines = [x.replace('\n', '')  for x in beforeLines]
    afterLines = after.splitlines(1)
    afterLines = [x.replace('\n', '')  for x in afterLines]
    dif = list(Differ().compare(beforeLines, afterLines))
    removedLines = [line.strip("- ").strip("\n") for line in dif if line[0] == '-']
    if removedLines:
        log("\nREMOVED:\n"+beforeLines[0].strip("\n"), logman)
        for line in removedLines:
            if not "rows" in line:
                log(line, logman)
        log("\n", logman)

def clean_backup_catalog(minRetainedBackups, minRetainedDays, deleteBackups, outputCatalog, outputDeletedCatalog, outputNDeletedLBEntries, sqlman, logman):  
    if outputCatalog or outputDeletedCatalog:
        #nCatalogEntries = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select count(*) from sys.m_backup_catalog\"", shell=True).strip(' '))
        nCatalogEntries = int(run_command(sqlman.hdbsql_jAQaxU + " \"select count(*) from sys.m_backup_catalog\"").strip(' '))
        if nCatalogEntries > 100000:
            log("INPUT ERROR: Please do not use -br true or -bo true if your backup catalog is larger than 100000 entries!", logman, True)
            os._exit(1)
    #nDataBackupCatalogEntriesBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name != 'log backup'\"", shell=True).strip(' '))      
    nDataBackupCatalogEntriesBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name != 'log backup'\"").strip(' '))
    nLogBackupCatalogEntriesBefore = 0
    if outputNDeletedLBEntries:
        #nLogBackupCatalogEntriesBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name = 'log backup'\"", shell=True).strip(' '))
        nLogBackupCatalogEntriesBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name = 'log backup'\"").strip(' '))
    if nDataBackupCatalogEntriesBefore == 0:
        return [0,0]
    sqls_for_cleanup = sqls_for_backup_catalog_cleanup(minRetainedBackups, minRetainedDays, deleteBackups, sqlman)
    if sqls_for_cleanup:
        sql_for_catalog = "select ENTRY_ID, ENTRY_TYPE_NAME, BACKUP_ID, SYS_START_TIME, STATE_NAME from sys.m_backup_catalog"
        if outputCatalog or outputDeletedCatalog:
            #beforeCatalog = subprocess.check_output(sqlman.hdbsql_jAxU + " \"" + sql_for_catalog + "\"", shell=True)
            beforeCatalog = run_command(sqlman.hdbsql_jAxU + " \"" + sql_for_catalog + "\"")
        if outputCatalog:
            log("\nBEFORE:\n"+beforeCatalog, logman)
        for sql_for_cleanup in sqls_for_cleanup:
            errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not clean backup catalog. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the system privilege BACKUP ADMIN.\n"
            errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql_for_cleanup+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
            try_execute_sql(sql_for_cleanup, errorlog, sqlman, logman)                 
        #nDataBackupCatalogEntriesAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name != 'log backup'\"", shell=True).strip(' '))
        nDataBackupCatalogEntriesAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name != 'log backup'\"").strip(' '))
        nLogBackupCatalogEntriesAfter = 0
        if outputNDeletedLBEntries:
            #nLogBackupCatalogEntriesAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name = 'log backup'\"", shell=True).strip(' '))
            nLogBackupCatalogEntriesAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_backup_catalog where entry_type_name = 'log backup'\"").strip(' '))
        if outputCatalog or outputDeletedCatalog:
            #afterCatalog = subprocess.check_output(sqlman.hdbsql_jAxU + " \"" + sql_for_catalog + "\"", shell=True)
            afterCatalog = run_command(sqlman.hdbsql_jAxU + " \"" + sql_for_catalog + "\"")
        if outputCatalog:
            log("\nAFTER:\n"+afterCatalog, logman)
        if outputDeletedCatalog:
            print_removed_entries(beforeCatalog, afterCatalog, logman)
        return [nDataBackupCatalogEntriesBefore - nDataBackupCatalogEntriesAfter, max(nLogBackupCatalogEntriesBefore - nLogBackupCatalogEntriesAfter,0)] #if a logbackup was done during run
    else:
        return [0,0]

def clear_traces(trace_list, oldestRetainedTraceContentDate, backupTraceContent, sqlman, logman):
    if backupTraceContent:
        sql = "ALTER SYSTEM CLEAR TRACES ("+trace_list+") UNTIL '"+oldestRetainedTraceContentDate.strftime('%Y-%m-%d')+" "+datetime.now().strftime("%H:%M:%S")+"' WITH BACKUP" 
    else:
        sql = "ALTER SYSTEM CLEAR TRACES ("+trace_list+") UNTIL '"+oldestRetainedTraceContentDate.strftime('%Y-%m-%d')+" "+datetime.now().strftime("%H:%M:%S")+"'" 
    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not clear traces. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the system privilege TRACE ADMIN.\n"
    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
    try_execute_sql(sql, errorlog, sqlman, logman)    
     
def clean_trace_files(retainedTraceContentDays, retainedExpensiveTraceContentDays, backupTraceContent, backupTraceDirectory, timeOutForMove, retainedTraceFilesDays, outputTraces, outputRemovedTraces, SID, DATABASE, local_dbinstance, hosts, sqlman, logman):
    #nbrTracesBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_tracefiles\"", shell=True).strip(' '))
    nbrTracesBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_tracefiles\"").strip(' '))
    if nbrTracesBefore == 0:
        return 0  
    if outputTraces:
        #beforeTraces = subprocess.check_output(sqlman.hdbsql_jAxU + " \"select * from sys.m_tracefiles order by file_mtime desc\"", shell=True)
        beforeTraces = run_command(sqlman.hdbsql_jAxU + " \"select * from sys.m_tracefiles order by file_mtime desc\"")
        log("\nBEFORE:\n"+beforeTraces, logman)
    if outputRemovedTraces:
        #beforeTraceFiles = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"select HOST, FILE_NAME from sys.m_tracefiles order by file_mtime desc\"", shell=True)
        beforeTraceFiles = run_command(sqlman.hdbsql_jAaxU + " \"select HOST, FILE_NAME from sys.m_tracefiles order by file_mtime desc\"")
    if retainedTraceContentDays != "-1" or retainedExpensiveTraceContentDays != "-1":
        oldestRetainedTraceContentDate = datetime.now() + timedelta(days = -int(retainedTraceContentDays))
        timeStampsForClearTraces = [datetime.now().strftime("%Y%m%d%H%M%S"), (datetime.now() + timedelta(seconds=1)).strftime("%Y%m%d%H%M%S"), (datetime.now() + timedelta(seconds=2)).strftime("%Y%m%d%H%M%S"), (datetime.now() + timedelta(seconds=3)).strftime("%Y%m%d%H%M%S")]
        if retainedTraceContentDays != "-1":
            clear_traces("'ALERT','CLIENT','CRASHDUMP','EMERGENCYDUMP','RTEDUMP','UNLOAD','ROWSTOREREORG','SQLTRACE','*'", oldestRetainedTraceContentDate, backupTraceContent, sqlman, logman)  
        if retainedExpensiveTraceContentDays != "-1":   # internal incident 1980358670, SAP Note 2819941 shows a BUG that should be fixed! "expected behaviour" = bull s###
            clear_traces("'EXPENSIVESTATEMENT'", oldestRetainedTraceContentDate, backupTraceContent, sqlman, logman)  
        if backupTraceDirectory:
            if not DATABASE:
                log("INPUT ERROR: If -tbd is used, either DATABASE must be specified in the key (see the manual of hdbuserstore), or -dbs must be specifed.", logman)
                log("NOTE: -tbd is not supported for none MDC systems", logman, True)
                os._exit(1)
            if not os.path.exists(backupTraceDirectory):
                os.makedirs(backupTraceDirectory)
            fileNameEndingsToBeMoved = ['_'+timeStamp+'.gz' for timeStamp in timeStampsForClearTraces]
            fileNameEndingsToWaitFor = ['_'+timeStamp+'.2gz' for timeStamp in timeStampsForClearTraces]
            #This compression takes a while and .2gz files are created as intermediate files, when the .2gz files are gone, we are done
            filesToWaitFor = ['dummy.2gz']
            waitedSeconds = 0
            while filesToWaitFor and waitedSeconds < timeOutForMove:
                time.sleep(1)
                waitedSeconds += 1
                sql = "select FILE_NAME from sys.m_tracefiles where FILE_NAME like '%" + "' or FILE_NAME like '%".join(fileName for fileName in fileNameEndingsToWaitFor) + "'"
                #filesToWaitFor = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"" + sql + "\"", shell=True).splitlines(1)
                filesToWaitFor = run_command(sqlman.hdbsql_jAQaxU + " \"" + sql + "\"").splitlines(1)
                filesToWaitFor = [file.strip('\n').strip(' ') for file in filesToWaitFor]
                filesToWaitFor = [file for file in filesToWaitFor if file]
            if waitedSeconds < timeOutForMove:
                sql = "select FILE_NAME from sys.m_tracefiles where FILE_NAME like '%" + "' or FILE_NAME like '%".join(fileName for fileName in fileNameEndingsToBeMoved) + "'"
                #filesToBeMoved = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"" + sql + "\"", shell=True).splitlines(1)
                filesToBeMoved = run_command(sqlman.hdbsql_jAQaxU + " \"" + sql + "\"").splitlines(1)
                filesToBeMoved = [file.strip('\n').strip(' ') for file in filesToBeMoved]
                filesToBeMoved = [file for file in filesToBeMoved if file]
                for host in hosts:
                    path = cdalias('cdhdb', local_dbinstance)+"/"+host 
                    for filename in filesToBeMoved:
                        #fullFileName = subprocess.check_output("find "+path+" -name "+filename, shell=True).strip('\n').strip(' ')
                        fullFileName = run_command("find "+path+" -name "+filename).strip('\n').strip(' ')
                        if fullFileName and ((DATABASE == 'SYSTEMDB' and 'DB_' in fullFileName) or (DATABASE != 'SYSTEMDB' and not 'DB_'+DATABASE in fullFileName)):
                            fullFileName = ''
                        if fullFileName:
                            #subprocess.check_output("mv "+fullFileName+" "+backupTraceDirectory+"/", shell=True)
                            dummyout = run_command("mv "+fullFileName+" "+backupTraceDirectory+"/")
                if outputRemovedTraces and filesToBeMoved:
                    log("\nARCHIVED ("+str(len(filesToBeMoved))+"):", logman)
                    for filename in filesToBeMoved:
                        log(filename, logman)
            else:
                log("WARNING: The compression, requested by the -tbd flag, took longer ("+str(waitedSeconds)+" seconds) than the timeout ("+str(timeOutForMove)+" seconds), so the archived trace files will not be moved.")
    if retainedTraceFilesDays != "-1":
        oldestRetainedTraceFilesDate = datetime.now() + timedelta(days = -int(retainedTraceFilesDays))
        sql = "select FILE_NAME from sys.m_tracefiles where file_size != '-1' and file_mtime < '"+oldestRetainedTraceFilesDate.strftime('%Y-%m-%d')+" "+datetime.now().strftime("%H:%M:%S")+"'"  # file_size = -1 --> folder, cannot be removed
        #filesToBeRemoved = subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"" + sql + "\"", shell=True).splitlines(1)
        filesToBeRemoved = run_command(sqlman.hdbsql_jAQaxU + " \"" + sql + "\"").splitlines(1)
        filesToBeRemoved = [file.strip('\n').strip(' ') for file in filesToBeRemoved if file != '\n'] 
        # Ignore files with names that breaks the ALTER command, or kill.sap according to SAP Note 2349144, and backup.log and backint.log since they are taken care of by -zb, see SAP Note 2431472 about hdbdaemon, we do not want to delete any .sem or .status file, and we do not want to delete any links, e.g. .sap<SID>_HDB<inst>, and we dont want to delete the log for the webdispatcher
        filesToBeRemoved = [file for file in filesToBeRemoved if not (" " in file or "," in file or "'" in file or "kill.sap" in file or "backup.log" in file or "backint.log" in file or "hdbdaemon.status" in file or "sapstart.sem" in file or "sapstart.log" in file or ".sap"+SID+"_HDB"+local_dbinstance in file or "http_fe.log" in file)]
        # Make sure we only delete files with known extensions (we dont delete .sem or .status files). Added two files without extensions that we want to delete. To delete files like dev_icm_sec one have to run HANACleaner as dev_icm_sec from SYSTEMDB, otherwise they are not in m_tracefiles
        filesToBeRemoved = [file for file in filesToBeRemoved if any(x in file for x in [".trc", ".log", ".stat", ".py", ".tpt", ".gz", ".zip", ".old", ".xml", ".txt", ".docs", ".cfg", ".dmp", ".cockpit", ".xs", "dev_icm_sec", "wdisp_icm_log"])] 
        if filesToBeRemoved:  # otherwise no file to remove
            filesToBeRemoved = [filesToBeRemoved[i:i + 100] for i in range(0, len(filesToBeRemoved), 100)]  #make sure we do not send too long statement, it could cause an error
            for files in filesToBeRemoved:
                filesToBeRemovedString = "'"+"', '".join(files)+"'"
                for host in hosts:
                    sql = "ALTER SYSTEM REMOVE TRACES (" +"'"+host+"', "+filesToBeRemovedString+ ")"
                    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not remove traces. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the system privilege TRACE ADMIN.\n"
                    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
                    try_execute_sql(sql, errorlog, sqlman, logman)        
    #nbrTracesAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_tracefiles\"", shell=True).strip(' '))                       
    nbrTracesAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.m_tracefiles\"").strip(' '))
    nbrRemovedTraceFiles = nbrTracesBefore - nbrTracesAfter
    if outputTraces:
        #afterTraces = subprocess.check_output(sqlman.hdbsql_jAxU + " \"select * from sys.m_tracefiles order by file_mtime desc\"", shell=True)
        afterTraces = run_command(sqlman.hdbsql_jAxU + " \"select * from sys.m_tracefiles order by file_mtime desc\"")
        log("\nAFTER:\n"+afterTraces, logman)
    if outputRemovedTraces and nbrRemovedTraceFiles:
        #afterTraceFiles = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"select HOST, FILE_NAME from sys.m_tracefiles order by file_mtime desc\"", shell=True)
        afterTraceFiles = run_command(sqlman.hdbsql_jAaxU + " \"select HOST, FILE_NAME from sys.m_tracefiles order by file_mtime desc\"")
        output_removed_trace_files(beforeTraceFiles, afterTraceFiles, logman)
    return nbrRemovedTraceFiles

def clean_dumps(retainedDumpDays, local_dbinstance, sqlman, logman):
    path = cdalias('cdglo', local_dbinstance)+"/sapcontrol/snapshots/" 
    with open(os.devnull, 'w') as devnull:
        #nbrDumpsBefore = int(subprocess.check_output("ls "+path+"fullsysteminfodump* | wc -l", shell=True, stderr=devnull).strip(' '))
        nbrDumpsBefore = int(run_command("ls "+path+"fullsysteminfodump* | wc -l").strip(' ')) #this might be a problem ... from https://docs.python.org/3/library/subprocess.html#subprocess.getoutput : 
        #The stdout and stderr arguments may not be supplied at the same time as capture_output. If you wish to capture and combine both streams into one, use stdout=PIPE and stderr=STDOUT instead of capture_output.
        if not nbrDumpsBefore:
            return 0
        if sqlman.log:
            log("find "+path+"fullsysteminfodump* -mtime +"+retainedDumpDays+" -delete", logman)
        if sqlman.execute:
            #subprocess.check_output("find "+path+"fullsysteminfodump* -mtime +"+retainedDumpDays+" -delete", shell=True, stderr=devnull)
            dummyout = run_command("find "+path+"fullsysteminfodump* -mtime +"+retainedDumpDays+" -delete")
        #nbrDumpsAfter = int(subprocess.check_output("ls "+path+"fullsysteminfodump* | wc -l", shell=True, stderr=devnull).strip(' ')) 
        nbrDumpsAfter = int(run_command("ls "+path+"fullsysteminfodump* | wc -l").strip(' ')) 
    return nbrDumpsBefore - nbrDumpsAfter
           

def output_removed_trace_files(before, after, logman):
    beforeLines = before.splitlines(1)
    afterLines = after.splitlines(1) 
    beforeFiles = [line.strip('\n').strip('|').strip(' ') for line in beforeLines]
    afterFiles = [line.strip('\n').strip('|').strip(' ') for line in afterLines]    
    nbrTracesBefore = len(beforeFiles)
    nbrTracesAfter = len(afterFiles)
    nbrRemoved = nbrTracesBefore - nbrTracesAfter
    if nbrRemoved > 0:    
        log("\nREMOVED ("+str(nbrRemoved)+"):", logman)
        for beforeFile in beforeFiles:
            if beforeFile not in afterFiles:
                log(beforeFile, logman)
        log("\n", logman)

def clean_alerts(minRetainedAlertDays, outputAlerts, outputDeletedAlerts, sqlman, logman):
    try:
        #nbrAlertsBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _sys_statistics.statistics_alerts_base\"", shell=True).strip(' '))
        nbrAlertsBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _sys_statistics.statistics_alerts_base\"").strip(' '))
    except: 
        log("\nERROR: The user represented by the key "+sqlman.key+" could not find amount of alerts. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege SELECT on the table _sys_statistics.statistics_alerts_base.\n", logman, True)
        os._exit(1)
    if nbrAlertsBefore > 10000 and (outputAlerts or outputDeletedAlerts):
        outputAlerts = False
        outputDeletedAlerts = False
        log("INFO: The flags -ao and -ad were changed to false since there are too many alerts for printout.", logman)
    if outputAlerts or outputDeletedAlerts:
        #beforeAlerts = subprocess.check_output(sqlman.hdbsql_jAxU + " \"select SNAPSHOT_ID, ALERT_ID, ALERT_TIMESTAMP, ALERT_RATING from _SYS_STATISTICS.STATISTICS_ALERTS_BASE\"", shell=True)
        beforeAlerts = run_command(sqlman.hdbsql_jAxU + " \"select SNAPSHOT_ID, ALERT_ID, ALERT_TIMESTAMP, ALERT_RATING from _SYS_STATISTICS.STATISTICS_ALERTS_BASE\"")
    if outputAlerts:
        log("\nBEFORE:\n"+beforeAlerts, logman)
    sql = "DELETE FROM _SYS_STATISTICS.STATISTICS_ALERTS_BASE WHERE ALERT_TIMESTAMP < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(minRetainedAlertDays)+")"
    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete alerts. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege DELETE on the table _sys_statistics.statistics_alerts_base.\n"
    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
    try_execute_sql(sql, errorlog, sqlman, logman)     
    #nbrAlertsAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _sys_statistics.statistics_alerts_base\"", shell=True).strip(' '))
    nbrAlertsAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _sys_statistics.statistics_alerts_base\"").strip(' '))
    if outputAlerts or outputDeletedAlerts:
        #afterAlerts = subprocess.check_output(sqlman.hdbsql_jAxU + " \"select SNAPSHOT_ID, ALERT_ID, ALERT_TIMESTAMP, ALERT_RATING from _SYS_STATISTICS.STATISTICS_ALERTS_BASE\"", shell=True)
        afterAlerts = run_command(sqlman.hdbsql_jAxU + " \"select SNAPSHOT_ID, ALERT_ID, ALERT_TIMESTAMP, ALERT_RATING from _SYS_STATISTICS.STATISTICS_ALERTS_BASE\"")
    if outputAlerts:
        log("\nAFTER:\n"+afterAlerts, logman)
    if outputDeletedAlerts:
        print_removed_entries(beforeAlerts, afterAlerts, logman)
    return nbrAlertsBefore - nbrAlertsAfter
    
def clean_ini(minRetainedIniDays, version, revision, mrevision, sqlman, logman):
    if version < 2 or revision < 30:
        log("\nERROR: the -ir flag is only supported starting with SAP HANA 2.0 SPS03. You run on SAP HANA "+str(version)+" revision "+str(revision)+" maintenance revision "+str(mrevision), logman, True)
        os._exit(1)
    if version > 4:
        log("\nERROR: the -ir flag is not supported any more with SAP HANA 2.0 SPS05. You run on SAP HANA "+str(version)+" revision "+str(revision)+" maintenance revision "+str(mrevision), logman, True)
        # compare https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.05/en-US/fb097f2620c645d18064ce6b93c24a1e.html
        # with https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.04/en-US/fb097f2620c645d18064ce6b93c24a1e.html 
        os._exit(1)
    try:
        #nbrIniHistBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_INIFILE_CONTENT_HISTORY\"", shell=True).strip(' '))
        nbrIniHistBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_INIFILE_CONTENT_HISTORY\"").strip(' '))
    except: 
        log("\nERROR: The user represented by the key "+sqlman.key+" could not find amount of inifile history. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege SELECT on the view SYS.M_INIFILE_CONTENT_HISTORY.\n", logman, True)
        os._exit(1)
    d = datetime.today() - timedelta(days=minRetainedIniDays)
    sql = "ALTER SYSTEM CLEAR INIFILE CONTENT HISTORY UNTIL '"+str(d)+"'"
    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete inifile history. \nOne possible reason for this is insufficient privilege.\n"
    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
    try_execute_sql(sql, errorlog, sqlman, logman)     
    #nbrIniHistAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_INIFILE_CONTENT_HISTORY\"", shell=True).strip(' '))
    nbrIniHistAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_INIFILE_CONTENT_HISTORY\"").strip(' '))
    return nbrIniHistBefore - nbrIniHistAfter

def clean_objlock(minRetainedObjLockDays, sqlman, logman):
    try:
        sql = "select count(*) FROM _SYS_STATISTICS.HOST_OBJECT_LOCK_STATISTICS_BASE WHERE OBJECT_NAME = '(unknown)'"
        #nbrObjLockBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \""+sql+"\"", shell=True).strip(' '))
        nbrObjLockBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \""+sql+"\"").strip(' '))
    except: 
        errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not select object locks. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege SELECT on the table _SYS_STATISTICS.HOST_OBJECT_LOCK_STATISTICS_BASE.\n"
        errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
        log(errorlog, logman)
    sql = "DELETE FROM _SYS_STATISTICS.HOST_OBJECT_LOCK_STATISTICS_BASE WHERE OBJECT_NAME = '(unknown)' and SERVER_TIMESTAMP < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(minRetainedObjLockDays)+")"
    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete object locks. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege DELETE on the table _SYS_STATISTICS.HOST_OBJECT_LOCK_STATISTICS_BASE.\n"
    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
    try_execute_sql(sql, errorlog, sqlman, logman)         
    #nbrObjLockAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select count(*) FROM _SYS_STATISTICS.HOST_OBJECT_LOCK_STATISTICS_BASE WHERE OBJECT_NAME = '(unknown)'\"", shell=True).strip(' '))
    nbrObjLockAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"select count(*) FROM _SYS_STATISTICS.HOST_OBJECT_LOCK_STATISTICS_BASE WHERE OBJECT_NAME = '(unknown)'\"").strip(' '))
    return nbrObjLockBefore - nbrObjLockAfter

def clean_objhist(objHistMaxSize, outputObjHist, sqlman, logman):
    try:
        #objHistSizeBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select disk_size from SYS.M_TABLE_PERSISTENCE_LOCATION_STATISTICS where table_name = 'OBJECT_HISTORY'\"", shell=True).strip(' '))
        objHistSizeBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"select disk_size from SYS.M_TABLE_PERSISTENCE_LOCATION_STATISTICS where table_name = 'OBJECT_HISTORY'\"").strip(' '))
    except: 
        log("\nERROR: The user represented by the key "+sqlman.key+" could not find size of object history. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege SELECT on the table SYS.M_TABLE_PERSISTENCE_LOCATION_STATISTICS.\n", logman, True)
        os._exit(1)  
    if objHistSizeBefore > objHistMaxSize*1000000:   #mb --> b 
        sql = "DELETE FROM _SYS_REPO.OBJECT_HISTORY WHERE (package_id, object_name, object_suffix, version_id) NOT IN (SELECT package_id, object_name, object_suffix, MAX(version_id) AS maxvid from _SYS_REPO.OBJECT_HISTORY GROUP BY package_id, object_name, object_suffix ORDER BY package_id, object_name, object_suffix)"
        errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not clean the object history. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege DELETE on the table _SYS_REPO.OBJECT_HISTORY.\n"
        errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
        try_execute_sql(sql, errorlog, sqlman, logman)  
    #objHistSizeAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select disk_size from SYS.M_TABLE_PERSISTENCE_LOCATION_STATISTICS where table_name = 'OBJECT_HISTORY'\"", shell=True).strip(' '))
    objHistSizeAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"select disk_size from SYS.M_TABLE_PERSISTENCE_LOCATION_STATISTICS where table_name = 'OBJECT_HISTORY'\"").strip(' '))
    if outputObjHist:
        log("Object History was:"+str(objHistSizeBefore/1000000)+" mb and is now "+str(objHistSizeAfter/1000000)+" mb.", logman)
    return (objHistSizeBefore - objHistSizeAfter)/1000000  

def max_filesystem_usage_in_percent(file_system, ignore_filesystems, logman):
    log("Will now check most used memory in the file systems. If it hangs there is an issue with  df -h, then see if the -fs flag helps.", logman)
    maxPercentage = 0
    lines = None 
    try:
        #lines = subprocess.check_output("df -h -P -x fuse.gvfs-fuse-daemon "+file_system, shell=True).splitlines(1)
        lines = run_command("df -h -P -x fuse.gvfs-fuse-daemon "+file_system).splitlines(1)   # -x: telling df to ignore /root/.gvfs since normally <sid>adm lacking permissions, -P: Force output in one line for RedHat        
    except:
        log("WARNING: Something went wrong executing df -h, \n therefore the most used memory in your file systems will not be checked. \n As a workaround it is possible to use the -fs flag to only take into account the most relevant file system.", logman)
    if lines:
        log("The most used filesystem is using ", logman)
        used_percentages = []
        for line in lines:
            if not "Filesystem" in line and not "S.ficheros" in line and not "Dateisystem" in line:   # english, spanish, german and ...
                words = line.split()
                if len(words) == 6:
                    filesystem_and_mount = [words[0].strip('\n'), words[5].strip('\n')]
                elif len(words) == 5:
                    filesystem_and_mount = [words[0].strip('\n'), words[4].strip('\n')]
                else:
                    log("ERROR, Unexpted number output columns from df -h: \n"+line, logman)
                if len(words) == 6:
                    percentage = int(words[4].strip('%'))
                if len(words) == 5:
                    percentage = int(words[3].strip('%'))
                if len(words) > 1 and filesystem_and_mount[0] not in ignore_filesystems and filesystem_and_mount[1] not in ignore_filesystems:
                    used_percentages.append(percentage)
        maxPercentage = max(used_percentages)
        log(str(maxPercentage)+"%", logman) 
    return maxPercentage

def find_all(name, path, zipLinks):
    result = []
    if zipLinks:
        pathes = os.walk(path, followlinks=True)
    else:
        pathes = os.walk(path)
    for root, dirs, files in pathes:
        if name in files:
            result.append(os.path.join(root, name))
    return result
    
def getNbrRows(schema, table, sqlman):
    #return int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM "+schema+"."+table+" \"", shell=True).strip(' '))
    return int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM "+schema+"."+table+" \"").strip(' '))

def getAdapterName(schema, table, sqlman):
    #return subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT R.ADAPTER_NAME FROM SYS.REMOTE_SOURCES R JOIN SYS.VIRTUAL_TABLES V ON R.REMOTE_SOURCE_NAME = V.REMOTE_SOURCE_NAME WHERE V.SCHEMA_NAME = '"+schema+"' and TABLE_NAME = '"+table+"'\"", shell=True).strip(' ')
    return run_command(sqlman.hdbsql_jAQaxU + " \"SELECT R.ADAPTER_NAME FROM SYS.REMOTE_SOURCES R JOIN SYS.VIRTUAL_TABLES V ON R.REMOTE_SOURCE_NAME = V.REMOTE_SOURCE_NAME WHERE V.SCHEMA_NAME = '"+schema+"' and TABLE_NAME = '"+table+"'\"").strip(' ')

def zipBackupLogs(zipBackupLogsSizeLimit, zipBackupPath, zipLinks, zipOut, zipKeep, sqlman, logman):
    backup_log_pathes = find_all("backup.log", zipBackupPath, zipLinks)
    backint_log_pathes = find_all("backint.log", zipBackupPath, zipLinks)
    log_pathes = backup_log_pathes + backint_log_pathes
    nZipped = 0
    for aLog in log_pathes:
        if os.path.getsize(aLog)/1000000.0 > zipBackupLogsSizeLimit:
            newname = (aLog.strip(".log")+"_compressed_"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+".tar.gz").replace(":","-").replace(" ","_").replace("//", "/")
            tempname = newname.replace(".tar.gz", ".log")
            if sqlman.log:
                log("mv "+aLog+" "+tempname, logman)
                log("tar -czPf "+newname+" "+tempname, logman)      # P to avoid annoying error message
                log("rm "+tempname, logman)
                if not zipKeep:
                    log("rm "+newname, logman)
            if sqlman.execute:
                #subprocess.check_output("mv "+aLog+" "+tempname, shell=True)
                #subprocess.check_output("tar -czPf "+newname+" "+tempname, shell=True)      # P to avoid annoying error message
                #subprocess.check_output("rm "+tempname, shell=True)
                dummyout = run_command("mv "+aLog+" "+tempname)
                dummyout = run_command("tar -czPf "+newname+" "+tempname)      # P to avoid annoying error message
                dummyout = run_command("rm "+tempname)
                if zipOut:
                    log(aLog+" was compressed to "+newname+" and then removed", logman)
                nZipped += 1
                if not zipKeep:
                    #subprocess.check_output("rm "+newname, shell=True)
                    dummyout = run_command("rm "+newname)
    return nZipped
    
def cdalias(alias, local_dbinstance):   # alias e.g. cdtrace, cdhdb, ...
    su_cmd = ''
    #whoami = subprocess.check_output('whoami', shell=True).replace('\n','')
    whoami = run_command('whoami').replace('\n','')
    if whoami.lower() == 'root':
        sidadm = get_sid().lower()+'adm'
        su_cmd = 'su - '+sidadm+' '
    alias_cmd = su_cmd+'/bin/bash -l -c \'alias '+alias+'\''
    #command_run = subprocess.check_output(alias_cmd, shell=True)
    command_run = run_command(alias_cmd)
    pieces = re.sub(r'.*cd ','',command_run).strip("\n").strip("'").split("/")    #to remove ANSI escape codes (only needed in few systems)
    path = ''
    for piece in pieces:
        if piece and piece[0] == '$':
            piece_cmd = su_cmd+'/bin/bash -l -c'+" \' echo "+piece+'\''
            #piece = subprocess.check_output(piece_cmd, shell=True).strip("\n")
            piece = run_command(piece_cmd)
        path = path + '/' + piece + '/' 
    path = path.replace("[0-9][0-9]", local_dbinstance) # if /bin/bash shows strange HDB[0-9][0-9] we force correct instance on it
    return path

def reclaim_logsegments(maxFreeLogsegments, sqlman, logman):
    #nTotFreeLogsegmentsBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS WHERE STATE = 'Free'\"", shell=True, stderr=subprocess.STDOUT).strip(' '))
    nTotFreeLogsegmentsBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS WHERE STATE = 'Free'\"").strip(' ')) #this might be a problem ... from https://docs.python.org/3/library/subprocess.html#subprocess.getoutput : 
    #The stdout and stderr arguments may not be supplied at the same time as capture_output. If you wish to capture and combine both streams into one, use stdout=PIPE and stderr=STDOUT instead of capture_output.
    if nTotFreeLogsegmentsBefore == 0:
        return 0
    #listOfPorts = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"SELECT DISTINCT PORT FROM SYS.M_LOG_SEGMENTS\"", shell=True).splitlines(1)          
    listOfPorts = run_command(sqlman.hdbsql_jAaxU + " \"SELECT DISTINCT PORT FROM SYS.M_LOG_SEGMENTS\"").splitlines(1)
    listOfPorts = [port.strip('\n').strip('|').strip(' ') for port in listOfPorts]
    nFreeLogsegmentsPerServices = []
    for port in listOfPorts:
        #nFreeLogs = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS WHERE STATE = 'Free' AND PORT = '"+port+"'\"", shell=True).strip(' '))
        nFreeLogs = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS WHERE STATE = 'Free' AND PORT = '"+port+"'\"").strip(' '))
        nFreeLogsegmentsPerServices.append(nFreeLogs)
    if max(nFreeLogsegmentsPerServices) > maxFreeLogsegments:
        sql = "ALTER SYSTEM RECLAIM LOG"
        errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not reclaim logs. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the privilege LOG ADMIN.\n"
        errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
        try_execute_sql(sql, errorlog, sqlman, logman)     
    #nTotFreeLogsegmentsAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS WHERE STATE = 'Free'\"", shell=True).strip(' '))   
    nTotFreeLogsegmentsAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_LOG_SEGMENTS WHERE STATE = 'Free'\"").strip(' '))
    return nTotFreeLogsegmentsBefore - nTotFreeLogsegmentsAfter
    
    
def clean_events(minRetainedDaysForHandledEvents, minRetainedDaysForEvents, sqlman, logman):                                                #ignoring INFO events, due to bug in HANA (fixed be rev. ???)
    #nHandledEventsBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS WHERE STATE = 'HANDLED' and TYPE != 'INFO'\"", shell=True).strip(' '))
    nHandledEventsBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS WHERE STATE = 'HANDLED' and TYPE != 'INFO'\"").strip(' '))
    #nEventsBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS \"", shell=True).strip(' '))
    nEventsBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS \"").strip(' '))
    if nEventsBefore == 0:
        return [0,0,0,0]    
    oldestDayForKeepingHandledEvent = datetime.now() + timedelta(days = -int(minRetainedDaysForHandledEvents))
    #listOfHandledEventsToRemove = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, ID FROM SYS.M_EVENTS WHERE STATE = 'HANDLED' and TYPE != 'INFO' AND CREATE_TIME < '"+oldestDayForKeepingHandledEvent.strftime('%Y-%m-%d')+" 00:00:00'\"", shell=True).splitlines(1)
    listOfHandledEventsToRemove = run_command(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, ID FROM SYS.M_EVENTS WHERE STATE = 'HANDLED' and TYPE != 'INFO' AND CREATE_TIME < '"+oldestDayForKeepingHandledEvent.strftime('%Y-%m-%d')+" 00:00:00'\"").splitlines(1)
    listOfHandledEventsToRemove = [event.strip('\n').strip('|').split('|') for event in listOfHandledEventsToRemove]
    listOfHandledEventsToRemove = [[evComp.strip(' ') for evComp in event] for event in listOfHandledEventsToRemove]
    for event in listOfHandledEventsToRemove:
        sql1 = "ALTER SYSTEM SET EVENT ACKNOWLEDGED '"+event[0]+":"+event[1]+"' "+event[2]
        sql2 = "ALTER SYSTEM DELETE HANDLED EVENT '"+event[0]+":"+event[1]+"' "+event[2]
        errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete handled events. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the privilege MONITOR ADMIN.\n"
        errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql1+"\nand\n"+sql2+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
        try_execute_sql(sql1, errorlog, sqlman, logman)              
        try_execute_sql(sql2, errorlog, sqlman, logman)
    oldestDayForKeepingEvent = datetime.now() + timedelta(days = -int(minRetainedDaysForEvents))    
    #listOfEventsToRemove = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, ID, STATE FROM SYS.M_EVENTS WHERE TYPE != 'INFO' and CREATE_TIME < '"+oldestDayForKeepingEvent.strftime('%Y-%m-%d')+" 00:00:00'\"", shell=True).splitlines(1)
    listOfEventsToRemove = run_command(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, ID, STATE FROM SYS.M_EVENTS WHERE TYPE != 'INFO' and CREATE_TIME < '"+oldestDayForKeepingEvent.strftime('%Y-%m-%d')+" 00:00:00'\"").splitlines(1)
    listOfEventsToRemove = [event.strip('\n').strip('|').split('|') for event in listOfEventsToRemove]
    listOfEventsToRemove = [[evComp.strip(' ') for evComp in event] for event in listOfEventsToRemove]
    for event in listOfEventsToRemove:
        if event[3] != 'INFO': 
            sql1 = "ALTER SYSTEM SET EVENT ACKNOWLEDGED '"+event[0]+":"+event[1]+"' "+event[2]
            sql2 = "ALTER SYSTEM SET EVENT HANDLED '"+event[0]+":"+event[1]+"' "+event[2]
            sql3 = "ALTER SYSTEM DELETE HANDLED EVENT '"+event[0]+":"+event[1]+"' "+event[2]
            errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete events. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the privilege MONITOR ADMIN.\n"
            errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql1+"\nand\n"+sql2+"\nand\n"+sql3+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
            try_execute_sql(sql1, errorlog, sqlman, logman)              
            try_execute_sql(sql2, errorlog, sqlman, logman)
            try_execute_sql(sql3, errorlog, sqlman, logman) 
        else: # if STATE == INFO see SAP Note 2253869
            sql1 = "ALTER SYSTEM SET EVENT ACKNOWLEDGED '"+event[0]+":"+event[1]+"' "+event[2]
            sql2 = "ALTER SYSTEM DELETE HANDLED EVENT '"+event[0]+":"+event[1]+"' "+event[2]
            errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete events. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the privilege MONITOR ADMIN.\n"
            errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql1+"\nand\n"+sql2+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
            try_execute_sql(sql1, errorlog, sqlman, logman)              
            try_execute_sql(sql2, errorlog, sqlman, logman)             
    #nHandledEventsAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS WHERE STATE = 'HANDLED' and TYPE != 'INFO'\"", shell=True).strip(' '))
    nHandledEventsAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS WHERE STATE = 'HANDLED' and TYPE != 'INFO'\"").strip(' '))
    #nEventsAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS \"", shell=True).strip(' '))
    nEventsAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.M_EVENTS \"").strip(' '))    
    return [nHandledEventsBefore - nHandledEventsAfter, nEventsBefore - nEventsAfter, nEventsAfter, nHandledEventsAfter]


def clean_audit_logs(retainedAuditLogDays, sqlman, logman):  # for this, both Audit Admin and Audit Operator is needed
    #nbrLogsBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.audit_log\"", shell=True).strip(' '))
    nbrLogsBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.audit_log\"").strip(' '))
    if nbrLogsBefore == 0:
        return 0  
    oldestRetainedAuditContentDate = datetime.now() + timedelta(days = -int(retainedAuditLogDays))
    sql = "ALTER SYSTEM CLEAR AUDIT LOG UNTIL '"+oldestRetainedAuditContentDate.strftime('%Y-%m-%d')+" "+datetime.now().strftime("%H:%M:%S")+"'" 
    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not clear traces. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the system privilege AUDIT ADMIN and/or AUDIT OPERATOR.\n"
    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner."
    try_execute_sql(sql, errorlog, sqlman, logman)              
    #nbrLogsAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.audit_log\"", shell=True).strip(' '))       
    nbrLogsAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM sys.audit_log\"").strip(' '))
    return nbrLogsBefore - nbrLogsAfter    
        

def clean_pending_emails(pendingEmailsDays, sqlman, logman):
    #nbrEmailsBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _SYS_STATISTICS.STATISTICS_EMAIL_PROCESSING\"", shell=True).strip(' '))
    nbrEmailsBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _SYS_STATISTICS.STATISTICS_EMAIL_PROCESSING\"").strip(' '))
    if nbrEmailsBefore == 0:
        return 0
    sql = "DELETE FROM _SYS_STATISTICS.STATISTICS_EMAIL_PROCESSING WHERE SECONDS_BETWEEN(SNAPSHOT_ID, CURRENT_TIMESTAMP) > "+pendingEmailsDays+" * 86400"
    errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not delete pending emails. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the object privilege DELETE on the _SYS_STATISTICS schema.\n"
    errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner."
    try_execute_sql(sql, errorlog, sqlman, logman)                   
    #nbrEmailsAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _SYS_STATISTICS.STATISTICS_EMAIL_PROCESSING\"", shell=True).strip(' '))  
    nbrEmailsAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM _SYS_STATISTICS.STATISTICS_EMAIL_PROCESSING\"").strip(' '))
    return nbrEmailsBefore - nbrEmailsAfter    
          

def defragment(fragmentationLimit, outputFragmentation, sqlman, logman):
    #fragPerPortBefore = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, USED_SIZE, TOTAL_SIZE from SYS.M_VOLUME_FILES WHERE FILE_TYPE = 'DATA'\" ", shell=True).splitlines(1)
    fragPerPortBefore = run_command(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, USED_SIZE, TOTAL_SIZE from SYS.M_VOLUME_FILES WHERE FILE_TYPE = 'DATA'\" ").splitlines(1)
    fragPerPortBefore = [port.strip('\n').strip('|').split('|') for port in fragPerPortBefore]    
    fragPerPortBefore = [[elem.strip(' ') for elem in port] for port in fragPerPortBefore]    
    fragPerPortBefore = [port+[round(((float(port[3])-float(port[2]))/float(port[3])),2)*100] for port in fragPerPortBefore]  
    if outputFragmentation:
        log("\nBEFORE FRAGMENTATION:", logman)
        log("Host                Port                Used Space [B]                Total Space [B]               Fragmentation [%]", logman)
        for port in fragPerPortBefore:
            log(port[0]+" "*(20-len(port[0]))+port[1]+" "*(20-len(port[1]))+port[2]+" "*(30-len(port[2]))+port[3]+" "*(30-len(port[3]))+str(port[4]), logman)
        log("\n", logman)
    for port in fragPerPortBefore:
        if port[4] > fragmentationLimit:
            sql = "ALTER SYSTEM RECLAIM DATAVOLUME '"+port[0]+":"+port[1]+"' 120 DEFRAGMENT"
            errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not defragment the data volumes. \nOne possible reason for this is insufficient privilege, \ne.g. lack of the privilege RESOURCE ADMIN.\n"
            errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
            errorlog += "Note: If you use System Replication see Q19 in SAP Note 1999880"
            try_execute_sql(sql, errorlog, sqlman, logman)             
    #fragPerPortAfter = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, USED_SIZE, TOTAL_SIZE from SYS.M_VOLUME_FILES WHERE FILE_TYPE = 'DATA'\" ", shell=True).splitlines(1)
    fragPerPortAfter = run_command(sqlman.hdbsql_jAaxU + " \"SELECT HOST, PORT, USED_SIZE, TOTAL_SIZE from SYS.M_VOLUME_FILES WHERE FILE_TYPE = 'DATA'\" ").splitlines(1)
    fragPerPortAfter = [port.strip('\n').strip('|').split('|') for port in fragPerPortAfter]    
    fragPerPortAfter = [[elem.strip(' ') for elem in port] for port in fragPerPortAfter]    
    fragPerPortAfter = [port+[round(((float(port[3])-float(port[2]))/float(port[3])),2)*100] for port in fragPerPortAfter]        
    fragChange = []
    for i in range(len(fragPerPortBefore)):
        if fragPerPortBefore[i][4] > fragPerPortAfter[i][4]:
            fragChange.append([fragPerPortBefore[i][0], fragPerPortBefore[i][1], fragPerPortBefore[i][4] - fragPerPortAfter[i][4]])
        elif fragPerPortBefore[i][4] > fragmentationLimit:
            fragChange.append([fragPerPortBefore[i][0], fragPerPortBefore[i][1], 0])
    if outputFragmentation and fragChange:
        log("\nAFTER FRAGMENTATION:", logman)
        log("Host                Port                Used Space [B]                Total Space [B]               Fragmentation [%]", logman)
        for port in fragPerPortAfter:
            log(port[0]+" "*(20-len(port[0]))+port[1]+" "*(20-len(port[1]))+port[2]+" "*(30-len(port[2]))+port[3]+" "*(30-len(port[3]))+str(port[4]), logman)
        log("\n", logman)    
    return fragChange
    
def reclaim_rs_containers(outputRcContainers, sqlman, logman):
    #nTablesWithMultipleRSContainersBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(TABLE_NAME) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"", shell=True).strip(' '))
    nTablesWithMultipleRSContainersBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(TABLE_NAME) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"").strip(' '))
    #nContCount = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(CONTAINER_COUNT) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"", shell=True).strip(' '))    
    nContCount = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(CONTAINER_COUNT) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"").strip(' '))    
    nUnnecessaryRSContainersBefore = 0
    if nContCount:    
        #nUnnecessaryRSContainersBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT SUM(CONTAINER_COUNT) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"", shell=True).strip(' ')) - nTablesWithMultipleRSContainersBefore
        nUnnecessaryRSContainersBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT SUM(CONTAINER_COUNT) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"").strip(' ')) - nTablesWithMultipleRSContainersBefore
    #tablesWithMultipleRSContainers = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"SELECT SCHEMA_NAME, TABLE_NAME from SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\" ", shell=True).splitlines(1)
    tablesWithMultipleRSContainers = run_command(sqlman.hdbsql_jAaxU + " \"SELECT SCHEMA_NAME, TABLE_NAME from SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\" ").splitlines(1)
    tablesWithMultipleRSContainers = [port.strip('\n').strip('|').split('|') for port in tablesWithMultipleRSContainers]    
    tablesWithMultipleRSContainers = [[elem.strip(' ') for elem in port] for port in tablesWithMultipleRSContainers]   
    if nUnnecessaryRSContainersBefore > 0:
        for table in tablesWithMultipleRSContainers:
            sql = "ALTER TABLE "+table[0]+"."+table[1]+" RECLAIM DATA SPACE"
            errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not reclaim data space of the table "+table[0]+"."+table[1]+". \nOne possible reason for this is insufficient privilege, \ne.g. lack of ALTER privilege on the schema "+table[0]+".\n"
            errorlog += "Unfortunately there is NO nice way to give privileges to the DB User to be allowed to do this.\nEither you can run hanacleaner as SYSTEM user (NOT recommended) or grant DATA ADMIN to the user (NOT recommended).\n"               
            errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner"
            try_execute_sql(sql, errorlog, sqlman, logman)          
    #nTablesWithMultipleRSContainersAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(TABLE_NAME) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"", shell=True).strip(' '))
    nTablesWithMultipleRSContainersAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(TABLE_NAME) FROM SYS.M_RS_TABLES WHERE CONTAINER_COUNT > 1\"").strip(' '))
    if nTablesWithMultipleRSContainersAfter != 0:
        log("\nERROR: Something went wrong. After reclaim of multiple row store table containers we still have "+str(nTablesWithMultipleRSContainersAfter)+" tables with multiple row store containers. Please investigate.", logman, True)
        os._exit(1)
    return [str(nTablesWithMultipleRSContainersBefore), str(nUnnecessaryRSContainersBefore)]

def force_compression(maxRawComp, maxEstComp, maxRowComp, maxMemComp, minDistComp, maxQuotaComp, maxUDIVComp, maxBLOCKComp, partComp, mergeBeforeComp, version, revision, mrevision, outComp, sqlman, logman):
    #CREATE SQLS
    if not partComp:
        #Tables with no compression
        raw = "raw_record_count_in_main > "+str(maxRawComp)
        est = "estimated_max_memory_size_in_total > "+str(maxEstComp)+" * 1024 * 1024 * 1024"  #GB
        sql_nocomp = """select distinct schema_name, table_name from sys.m_cs_tables where last_compressed_record_count = 0 and """+est+""" and """+raw
        #Columns with default compression
        join = "inner join SYS.TABLE_COLUMNS TC on C.schema_name = TC.schema_name and C.table_name = TC.table_name and C.column_name = TC.column_name"
        default = "C.compression_type = 'DEFAULT'"
        dist = "C.distinct_count <= C.count * "+str(minDistComp/100.)
        count = "C.count >= "+str(maxRowComp)
        mem = "C.memory_size_in_total >= "+str(maxMemComp)+" * 1024 * 1024"   #MB
        gen = "TC.generation_type is NULL" # a generated column is typically a virtual column that doesn’t allocate space and so compression isn’t possible
        sql_default = """select distinct C.schema_name, C.table_name from SYS.M_CS_ALL_COLUMNS C """+join+""" where """+default+""" and """+dist+""" and """+count+""" and """+mem+""" and """+gen                
        #Tables with too much UDIVs
        quota = "max_udiv > "+str(maxQuotaComp)+" / 100. * (raw_record_count_in_main + raw_record_count_in_delta)"
        udivs = "max_udiv > "+str(maxUDIVComp)
        sql_udivs = """select distinct schema_name, table_name from sys.m_cs_tables where """+quota+""" and """+udivs
        #Columns with SPARE or PREFIXED
        if version < 2 and revision < 123 and mrevision < 3:  #the SPARSE if fixed with 122.02
            comp = "compression_type in ('SPARSE', 'PREFIXED')"
        else:
            comp = "compression_type = 'PREFIXED'"
        sql_block = """select distinct schema_name, table_name from SYS.M_CS_COLUMNS where index_type = 'BLOCK' and """+comp+""" and count > """+str(maxBLOCKComp)
    else:
        #Tables with no compression
        raw = "raw_main_count_all_partitions > "+str(maxRawComp)
        est = "max_mem_all_partitions > "+str(maxEstComp)+" * 1024 * 1024 * 1024"  #GB
        sql_int = """select schema_name, table_name, sum(estimated_max_memory_size_in_total) as max_mem_all_partitions, sum(raw_record_count_in_main) as raw_main_count_all_partitions from sys.m_cs_tables where last_compressed_record_count = 0 group by schema_name, table_name"""
        sql_nocomp = """select distinct schema_name, table_name from ("""+sql_int+""") where """+est+""" and """+raw 
        #Columns with default compression
        join = "inner join SYS.TABLE_COLUMNS TC on C.schema_name = TC.schema_name and C.table_name = TC.table_name and C.column_name = TC.column_name"
        default = "C.compression_type = 'DEFAULT'"  
        dist = "C.distinct_count <= C.count * "+str(minDistComp/100.)
        gen = "TC.generation_type is NULL" # a generated column is typically a virtual column that doesn’t allocate space and so compression isn’t possible  
        count = "count_all_partitions >= "+str(maxRowComp)
        mem = "memory_size_all_partitions >= "+str(maxMemComp)+" * 1024 * 1024"  # MB
        sql_int = """select C.schema_name as schema_name, C.table_name as table_name, sum(C.memory_size_in_total) as memory_size_all_partitions, sum(C.count) as count_all_partitions from SYS.M_CS_ALL_COLUMNS C """+join+""" where """+default+""" and """+dist+""" and """+gen+""" group by C.schema_name, C.table_name, C.column_name"""        
        sql_default = """select distinct schema_name, table_name from ("""+sql_int+""") where """+count+""" and """+mem
        #Tables with too much UDIVs
        quota = "udivs_all_partitions > "+str(maxQuotaComp)+" / 100. * (main_all_partitions + delta_all_partitions)"
        udivs = "udivs_all_partitions > "+str(maxUDIVComp)
        sql_int = """select schema_name, table_name, sum(max_udiv) as udivs_all_partitions, sum(raw_record_count_in_main) as main_all_partitions, sum(raw_record_count_in_delta) as delta_all_partitions from sys.m_cs_tables group by schema_name, table_name"""        
        sql_udivs = """select distinct schema_name, table_name from ("""+sql_int+""") where """+quota+""" and """+udivs
        #Columns with SPARE or PREFIXED
        if version < 2 and revision < 123 and mrevision < 3:  #the SPARSE is fixed with 122.02
            comp = "compression_type in ('SPARSE', 'PREFIXED')"
        else:
            comp = "compression_type = 'PREFIXED'"            
        sql_int = """select schema_name, table_name, column_name, sum(count) as count_all_partitions from SYS.M_CS_COLUMNS where index_type = 'BLOCK' and """+comp+""" group by schema_name, table_name, column_name"""
        sql_block = """select distinct schema_name, table_name from ( """+sql_int+""") where count_all_partitions > """+str(maxBLOCKComp)      
    #FIND TABLES TO COMPRESS
    #Tables with no compression
    tablesToCompress = []
    if all(c > -1 for c in [maxRawComp, maxEstComp]):
        #tablesToCompress = subprocess.check_output(sqlman.hdbsql_jAaxU + " \""+sql_nocomp+"\" ", shell=True).splitlines(1)
        tablesToCompress = run_command(sqlman.hdbsql_jAaxU + " \""+sql_nocomp+"\" ").splitlines(1)
        tablesToCompress = [table.strip('\n').strip('|').split('|') for table in tablesToCompress]    
        tablesToCompress = [[elem.strip(' ') for elem in table] for table in tablesToCompress]
    #Columns with default compression
    moreTablesToCompress = []
    if all(c > -1 for c in [maxRowComp, maxMemComp, minDistComp]):  
        #moreTablesToCompress = subprocess.check_output(sqlman.hdbsql_jAaxU + " \""+sql_default+"\" ", shell=True).splitlines(1)      
        moreTablesToCompress = run_command(sqlman.hdbsql_jAaxU + " \""+sql_default+"\" ").splitlines(1)
        moreTablesToCompress = [table.strip('\n').strip('|').split('|') for table in moreTablesToCompress]    
        moreTablesToCompress = [[elem.strip(' ') for elem in table] for table in moreTablesToCompress]
        for newtab in moreTablesToCompress:   
            if not newtab in tablesToCompress:
                tablesToCompress.append(newtab)
    #Tables with too much UDIVs
    moreTablesToCompress = []
    if all(c > -1 for c in [maxQuotaComp, maxUDIVComp]):        
        #moreTablesToCompress = subprocess.check_output(sqlman.hdbsql_jAaxU + " \""+sql_udivs+"\" ", shell=True).splitlines(1)
        moreTablesToCompress = run_command(sqlman.hdbsql_jAaxU + " \""+sql_udivs+"\" ").splitlines(1)
        moreTablesToCompress = [table.strip('\n').strip('|').split('|') for table in moreTablesToCompress]    
        moreTablesToCompress = [[elem.strip(' ') for elem in table] for table in moreTablesToCompress]
        for newtab in moreTablesToCompress:   
            if not newtab in tablesToCompress:
                tablesToCompress.append(newtab)
    #Columns with SPARE or PREFIXED
    moreTablesToCompress = []
    if maxBLOCKComp > -1:        
        #moreTablesToCompress = subprocess.check_output(sqlman.hdbsql_jAaxU + " \""+sql_block+"\" ", shell=True).splitlines(1)
        moreTablesToCompress = run_command(sqlman.hdbsql_jAaxU + " \""+sql_block+"\" ").splitlines(1)
        moreTablesToCompress = [table.strip('\n').strip('|').split('|') for table in moreTablesToCompress]    
        moreTablesToCompress = [[elem.strip(' ') for elem in table] for table in moreTablesToCompress]
        for newtab in moreTablesToCompress:   
            if not newtab in tablesToCompress:
                tablesToCompress.append(newtab)
    #COMPRESS (AND MERGE) TABLES
    failed = 0
    for tab in tablesToCompress:
        sql_merge = 'MERGE DELTA OF \\"'+tab[0]+'\\".\\"'+tab[1]+'\\"'     # necessary for tables starting with /
        errorlog_merge = "Failed to merge the table "+tab[0]+"."+tab[1]
        sql = """UPDATE \\\""""+tab[0]+"""\\\".\\\""""+tab[1]+"""\\\" WITH PARAMETERS ('OPTIMIZE_COMPRESSION' = 'FORCE')"""  # necessary for tables starting with /
        errorlog = "Failed to re-optimize the compression of the table "+tab[0]+"."+tab[1]
        succeeded_merge = True  # in case we will not merge before compression, we define merge to be success
        if mergeBeforeComp:
            [dummyout, succeeded_merge] = try_execute_sql(sql_merge, errorlog_merge, sqlman, logman, exit_on_fail = False)
        [dummyout, succeeded] = try_execute_sql(sql, errorlog, sqlman, logman, exit_on_fail = False)
        if not succeeded_merge or not succeeded:
            failed += 1
    if outComp:
        log("\n  ATTEMPTED COMPRESSION RE-OPTIMIZATION ON FOLLOWING TABLES:", logman)
        for tab in tablesToCompress:
            log("    "+tab[0]+"."+tab[1], logman)
        log("\n", logman)
    return [len(tablesToCompress), failed]
    
def create_vt_statistics(vtSchemas, defaultVTStatType, maxRowsForDefaultVT, largeVTStatType, otherDBVTStatType, ignore2ndMon, sqlman, logman):  #SAP Note 1872652: Creating statistics on a virtual table can be an expensive operation. 
    #Default statistics type: HISTOGRAM --> Creates a data statistics object that helps the query optimizer estimate the data distribution in a single-column data source
    #nVTs = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select count(*) from SYS.VIRTUAL_TABLES\"", shell=True).strip(' '))
    nVTs = int(run_command(sqlman.hdbsql_jAQaxU + " \"select count(*) from SYS.VIRTUAL_TABLES\"").strip(' '))
    #nVTsWithoutStatBefore = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select COUNT(*) from SYS.VIRTUAL_TABLES where TABLE_NAME NOT IN (select distinct DATA_SOURCE_OBJECT_NAME from SYS.DATA_STATISTICS)\"", shell=True).strip(' '))
    nVTsWithoutStatBefore = int(run_command(sqlman.hdbsql_jAQaxU + " \"select COUNT(*) from SYS.VIRTUAL_TABLES where TABLE_NAME NOT IN (select distinct DATA_SOURCE_OBJECT_NAME from SYS.DATA_STATISTICS)\"").strip(' '))
    if not nVTsWithoutStatBefore:
        return [nVTs, 0]
    #listOfVTsWithoutStat = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"select SCHEMA_NAME, TABLE_NAME from SYS.VIRTUAL_TABLES where TABLE_NAME NOT IN (select distinct DATA_SOURCE_OBJECT_NAME from SYS.DATA_STATISTICS)\"", shell=True).splitlines(1)
    listOfVTsWithoutStat = run_command(sqlman.hdbsql_jAaxU + " \"select SCHEMA_NAME, TABLE_NAME from SYS.VIRTUAL_TABLES where TABLE_NAME NOT IN (select distinct DATA_SOURCE_OBJECT_NAME from SYS.DATA_STATISTICS)\"").splitlines(1)
    listOfVTsWithoutStat = [vt.strip('\n').strip('|').split('|') for vt in listOfVTsWithoutStat]    
    listOfVTsWithoutStat = [[elem.strip(' ') for elem in vt] for vt in listOfVTsWithoutStat]       
    for vt in listOfVTsWithoutStat: 
        if not (ignore2ndMon and "_SYS_SR_SITE" in vt[0]):  #if ignore2ndMon (default true) then do not create statistics for the virtual tables in the _SYS_SR_SITE* schema
            if not vtSchemas or vt[0] in vtSchemas:  #if schemas for virtual tables are provided, then only consider these schemas for creating statistics
                statType = defaultVTStatType
                if maxRowsForDefaultVT > 0:
                    statType = defaultVTStatType if getNbrRows(vt[0], vt[1], sqlman) <= maxRowsForDefaultVT else largeVTStatType
                if otherDBVTStatType:
                    statType = statType if "hana" in getAdapterName(vt[0], vt[1], sqlman) else otherDBVTStatType
                #columns = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"select column_name from PUBLIC.TABLE_COLUMNS where table_name = '"+vt[1]+"' and schema_name = '"+vt[0]+"'\"", shell=True).splitlines(1)
                columns = run_command(sqlman.hdbsql_jAaxU + " \"select column_name from PUBLIC.TABLE_COLUMNS where table_name = '"+vt[1]+"' and schema_name = '"+vt[0]+"'\"").splitlines(1)
                columns =[col.strip('\n').strip('|').strip(' ') for col in columns]
                columns = '\\\", \\\"'.join(columns)                                                                                  # necessary for columns with mixed letter case
                sql = 'CREATE STATISTICS ON \\\"'+vt[0]+'\\\".\\\"'+vt[1]+'\\\" (\\\"'+columns+'\\\") TYPE '+statType                 # necessary for tables starting with / and for tables with mixed letter case 
                errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not create statistics on "+vt[0]+"."+vt[1]+". \nOne possible reason for this is insufficient privilege\n"
                errorlog += "\nTry, as the user represented by the key "+sqlman.key+" to simply do  SELECT * FROM "+vt[0]+"."+vt[1]+". If that does not work then it could be that the privileges of source system's technical user (used in the SDA setup) is not sufficient.\n"
                errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner\n"
                errorlog += "It could be that the respective ODBC driver was not properly set up. Please then follow the SAP HANA Administration Guide."
                try_execute_sql(sql, errorlog, sqlman, logman, exit_on_fail = False)  
    #nVTsWithoutStatAfter = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"select COUNT(*) from SYS.VIRTUAL_TABLES where TABLE_NAME NOT IN (select distinct DATA_SOURCE_OBJECT_NAME from SYS.DATA_STATISTICS)\"", shell=True).strip(' '))
    nVTsWithoutStatAfter = int(run_command(sqlman.hdbsql_jAQaxU + " \"select COUNT(*) from SYS.VIRTUAL_TABLES where TABLE_NAME NOT IN (select distinct DATA_SOURCE_OBJECT_NAME from SYS.DATA_STATISTICS)\"").strip(' '))
    return [nVTs, nVTsWithoutStatBefore - nVTsWithoutStatAfter]

def refresh_statistics(vtSchemas, refreshAge, ignore2ndMon, sqlman, logman):
    #nDSs = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.DATA_STATISTICS\"", shell=True).strip(' '))
    nDSs = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.DATA_STATISTICS\"").strip(' '))
    #nDSToRefresh_before = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.DATA_STATISTICS WHERE LAST_REFRESH_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(refreshAge)+")\"", shell=True).strip(' '))
    nDSToRefresh_before = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.DATA_STATISTICS WHERE LAST_REFRESH_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(refreshAge)+")\"").strip(' '))
    if not nDSToRefresh_before:
        return [nDSs, 0]
    #listOfDSsToRefresh = subprocess.check_output(sqlman.hdbsql_jAaxU + " \"select DATA_STATISTICS_SCHEMA_NAME, DATA_STATISTICS_NAME FROM SYS.DATA_STATISTICS WHERE LAST_REFRESH_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(refreshAge)+")\"", shell=True).splitlines(1)
    listOfDSsToRefresh = run_command(sqlman.hdbsql_jAaxU + " \"select DATA_STATISTICS_SCHEMA_NAME, DATA_STATISTICS_NAME FROM SYS.DATA_STATISTICS WHERE LAST_REFRESH_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(refreshAge)+")\"").splitlines(1)
    listOfDSsToRefresh = [ds.strip('\n').strip('|').split('|') for ds in listOfDSsToRefresh]    
    listOfDSsToRefresh = [[elem.strip(' ') for elem in ds] for ds in listOfDSsToRefresh] 
    for ds in listOfDSsToRefresh: 
        if not (ignore2ndMon and "_SYS_SR_SITE" in ds[0]):  #if ignore2ndMon (default true) then do not refresh statistics for the virtual tables in the _SYS_SR_SITE* schema
            if not vtSchemas or ds[0] in vtSchemas:  #if schemas for virtual tables are provided, then only consider these schemas for refreshing statistics
                sql = 'REFRESH STATISTICS \\\"'+ds[0]+'\\\".\\\"'+ds[1]+'\\\"'                 # necessary for tables starting with / and for tables with mixed letter case 
                errorlog = "\nERROR: The user represented by the key "+sqlman.key+" could not refresh statistics on "+ds[0]+"."+ds[1]+". \nOne possible reason for this is insufficient privilege\n"
                errorlog += "\nTry, as the user represented by the key "+sqlman.key+" to simply do  SELECT * FROM "+ds[0]+"."+ds[1]+". If that does not work then it could be that the privileges of source system's technical user (used in the SDA setup) is not sufficient.\n"
                errorlog += "If there is another error (i.e. not insufficient privilege) then please try to execute \n"+sql+"\nin e.g. the SQL editor in SAP HANA Studio. If you get the same error then this has nothing to do with hanacleaner\n"
                errorlog += "It could be that the respective ODBC driver was not properly set up. Please then follow the SAP HANA Administration Guide."
                try_execute_sql(sql, errorlog, sqlman, logman, exit_on_fail = False)  
    #nDSToRefresh_after = int(subprocess.check_output(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.DATA_STATISTICS WHERE LAST_REFRESH_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(refreshAge)+")\"", shell=True).strip(' '))
    nDSToRefresh_after = int(run_command(sqlman.hdbsql_jAQaxU + " \"SELECT COUNT(*) FROM SYS.DATA_STATISTICS WHERE LAST_REFRESH_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -"+str(refreshAge)+")\"").strip(' '))
    return [nDSs, nDSToRefresh_after - nDSToRefresh_before]

def clean_output(minRetainedOutputDays, sqlman, logman):
    path = logman.path
    nFilesBefore = len([name for name in os.listdir(path) if os.path.isfile(os.path.join(path, name))])
    if sqlman.log:
        log("find "+path+"/hanacleanerlog* -mtime +"+str(minRetainedOutputDays)+" -delete", logman)
    if sqlman.execute:
        #subprocess.check_output("find "+path+"/hanacleanerlog* -mtime +"+str(minRetainedOutputDays)+" -delete", shell=True)
        dummyout = run_command("find "+path+"/hanacleanerlog* -mtime +"+str(minRetainedOutputDays)+" -delete")
    nFilesAfter = len([name for name in os.listdir(path) if os.path.isfile(os.path.join(path, name))])
    return nFilesBefore - nFilesAfter  
    
def clean_anyfile(retainedAnyFileDays, anyFilePaths, anyFileWords, anyFileMaxDepth, sqlman, logman):
    removedFiles = 0
    if str(retainedAnyFileDays) == "0": #then dont use -mtime, dont work
        retainedAnyFileDaysString = ""
    else:
        retainedAnyFileDaysString = "-mtime +"+str(retainedAnyFileDays)
    for path, word in zip(anyFilePaths, anyFileWords):
        #nFilesBefore = int(subprocess.check_output("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -type f | wc -l", shell=True).strip(' '))
        nFilesBefore = int(run_command("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -type f | wc -l").strip(' '))
        with open(os.devnull, 'w') as devnull:
            if sqlman.log:
                log("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -name '*"+word+"*' -type f "+retainedAnyFileDaysString+" -delete", logman)
            if sqlman.execute:
                try:
                    #subprocess.check_output("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -name '*"+word+"*' -type f "+retainedAnyFileDaysString+" -delete", shell=True, stderr=devnull)
                    dummyout = run_command("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -name '*"+word+"*' -type f "+retainedAnyFileDaysString+" -delete")   #this might be a problem ... from https://docs.python.org/3/library/subprocess.html#subprocess.getoutput : 
                    #The stdout and stderr arguments may not be supplied at the same time as capture_output. If you wish to capture and combine both streams into one, use stdout=PIPE and stderr=STDOUT instead of capture_output.
                except:
                    pass   #File not  found, but no need to warn about that
        #nFilesAfter = int(subprocess.check_output("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -type f | wc -l", shell=True).strip(' '))
        nFilesAfter = int(run_command("find "+path+" -maxdepth "+str(anyFileMaxDepth)+" -type f | wc -l").strip(' '))
        removedFiles += nFilesBefore - nFilesAfter
    return removedFiles  
    
def checkAndConvertBooleanFlag(boolean, flagstring, logman = ''):     
    boolean = boolean.lower()
    if boolean not in ("false", "true"):
        if logman:
            log("INPUT ERROR: "+flagstring+" must be either 'true' or 'false'. Please see --help for more information.", logman, True)
        else:
            print("INPUT ERROR: "+flagstring+" must be either 'true' or 'false'. Please see --help for more information.")
        os._exit(1)
    boolean = True if boolean == "true" else False
    return boolean

def main():

    #####################  CHECK PYTHON VERSION ###########
    if sys.version_info[0] != 2 and sys.version_info[0] != 3:
        print("VERSION ERROR: hanacleaner is only supported for Python 2.7.x (for HANA 2 SPS05 and lower) and for Python 3.7.x (for HANA 2 SPS06 and higher). Did you maybe forget to log in as <sid>adm before executing this?")
        os._exit(1)
    if sys.version_info[0] == 3:
        print("VERSION WARNING: You are among the first using HANACleaner on Python 3. As always, use on your own risk, and please report issues to christian.hansen01@sap.com. Thank you!")

    #####################   DEFAULTS   ####################
    minRetainedBackups = "-1"
    minRetainedDays = "-1" #days
    deleteBackups = "false"
    outputCatalog = "false"
    outputDeletedCatalog = "false"
    outputNDeletedLBEntries = "true"
    backupTraceContent = "false"
    backupTraceDirectory = ""
    timeOutForMove = "30"
    outputTraces = "false"
    outputRemovedTraces = "false"
    zipBackupLogsSizeLimit = "-1" #mb
    zipBackupPath = '' #default will be set to cdtrace as soon as we get local_instance
    zipLinks = "false"
    zipOut = "false"
    zipKeep = "true"
    dbuserkeys = ["SYSTEMKEY"] # This/these KEY(S) has to be maintained in hdbuserstore  
                               # so that   hdbuserstore LIST    gives e.g. 
                               # KEY SYSTEMKEY
                               #     ENV : mo-fc8d991e0:30015
                               #     USER: SYSTEM
    dbases = ['']
    receiver_emails = None
    email_timeout = "-1"
    sendEmailSummary = "false" 
    email_client = ''   #default email client, mailx, will be specifed later if -enc not provided
    senders_email = None
    mail_server = None
    retainedTraceContentDays = "-1"
    retainedExpensiveTraceContentDays = "-1"
    retainedTraceFilesDays = "-1"
    retainedDumpDays = "-1"
    retainedAnyFileDays = "-1"
    anyFilePaths = [""]
    anyFileWords = [""]
    anyFileMaxDepth = "1"
    minRetainedAlertDays = "-1" #days
    minRetainedObjLockDays = "-1" #days
    outputAlerts = "false"
    outputDeletedAlerts = "false"
    objHistMaxSize = "-1" #mb  (default: not used)
    outputObjHist = "false"
    maxFreeLogsegments = "-1"
    minRetainedDaysForHandledEvents = "-1" #days
    minRetainedDaysForEvents = "-1" #days
    retainedAuditLogDays = "-1"
    pendingEmailsDays = "-1"
    fragmentationLimit = "-1" # percent
    outputFragmentation = "false"
    hanacleaner_interval = "-1"
    rcContainers = "false"
    outputRcContainers = "false"
    maxRawComp = '-1'  #number raw rows, e.g. 10000000
    maxEstComp = '-1'  #GB, e.g. 1
    maxRowComp = '-1'  #number rows, e.g. 10000000  
    maxMemComp = '-1'  #MB, e.g. 500
    minDistComp = '-1'   #%, e.g. 5
    maxQuotaComp = '-1'  #%, e.g. 150
    maxUDIVComp = '-1'  #number rows, e.g. 10000000
    maxBLOCKComp = '-1'  #number rows, e.g. 100000
    partComp = 'false' 
    mergeBeforeComp = 'false'
    outComp = 'false'
    createVTStat = 'false'
    defaultVTStatType = 'HISTOGRAM'
    maxRowsForDefaultVT = '-1'
    largeVTStatType = 'SIMPLE'
    otherDBVTStatType = ''
    vtSchemas = None
    ignore2ndMon = 'true'   #by default we ignore the secondary monitoring virtual tables
    refreshAge = '-1'
    minRetainedIniDays = "-1" #days
    file_system = "" # by default check all file systems with  df -h
    flag_files = []    #default: no configuration input file
    ignore_filesystems = ""
    execute_sql = 'true'
    out_sql = 'false'
    out_path = ""
    out_prefix = ""
    do_df_check = 'true'
    minRetainedOutputDays = "-1" #days
    out_config = 'false'
    online_test_interval = "-1" #seconds
    std_out = "true" #print to std out
    virtual_local_host = "" #default: assume physical local host
    ssl = "false"
    
    #####################  CHECK INPUT ARGUMENTS #################
    if len(sys.argv) == 1:
        print("INPUT ERROR: hanacleaner needs input arguments. Please see --help for more information.")
        os._exit(1) 
    if len(sys.argv) != 2 and len(sys.argv) % 2 == 0:
        print("INPUT ERROR: Wrong number of input arguments. Please see --help for more information.")
        os._exit(1)
    for i in range(len(sys.argv)):
        if i % 2 != 0:
            if sys.argv[i][0] != '-':
                print("INPUT ERROR: Every second argument has to be a flag, i.e. start with -. Please see --help for more information.")
                os._exit(1)

    ############ GET SID ##########
    SID = get_sid()

    #####################  PRIMARY INPUT ARGUMENTS   ####################
    flag_log = {}     
    if '-h' in sys.argv or '--help' in sys.argv:
        printHelp()   
    if '-d' in sys.argv or '--disclaimer' in sys.argv:
        printDisclaimer()
    flag_files = getParameterListFromCommandLine(sys.argv, '-ff', flag_log, flag_files)    
    flag_files = [ff.replace('%SID', SID) for ff in flag_files]

    ############ CONFIGURATION FILE ###################
    for flag_file in flag_files:
        with open(flag_file, 'r') as fin:
            for line in fin:
                firstWord = line.strip(' ').split(' ')[0]  
                if firstWord[0:1] == '-':
                    checkIfAcceptedFlag(firstWord)
                    flagValue = line.strip(' ').split('"')[1].strip('\n').strip('\r') if line.strip(' ').split(' ')[1][0] == '"' else line.strip(' ').split(' ')[1].strip('\n').strip('\r')
                    minRetainedBackups                = getParameterFromFile(firstWord, '-be', flagValue, flag_file, flag_log, minRetainedBackups)
                    minRetainedDays                   = getParameterFromFile(firstWord, '-bd', flagValue, flag_file, flag_log, minRetainedDays)
                    deleteBackups                     = getParameterFromFile(firstWord, '-bb', flagValue, flag_file, flag_log, deleteBackups)
                    outputCatalog                     = getParameterFromFile(firstWord, '-bo', flagValue, flag_file, flag_log, outputCatalog)
                    outputDeletedCatalog              = getParameterFromFile(firstWord, '-br', flagValue, flag_file, flag_log, outputDeletedCatalog)
                    outputNDeletedLBEntries           = getParameterFromFile(firstWord, '-bn', flagValue, flag_file, flag_log, outputNDeletedLBEntries)
                    retainedTraceContentDays          = getParameterFromFile(firstWord, '-tc', flagValue, flag_file, flag_log, retainedTraceContentDays)
                    retainedExpensiveTraceContentDays = getParameterFromFile(firstWord, '-te', flagValue, flag_file, flag_log, retainedExpensiveTraceContentDays)
                    backupTraceContent                = getParameterFromFile(firstWord, '-tcb', flagValue, flag_file, flag_log, backupTraceContent)
                    backupTraceDirectory              = getParameterFromFile(firstWord, '-tbd', flagValue, flag_file, flag_log, backupTraceDirectory)
                    timeOutForMove                    = getParameterFromFile(firstWord, '-tmo', flagValue, flag_file, flag_log, timeOutForMove)
                    retainedTraceFilesDays            = getParameterFromFile(firstWord, '-tf', flagValue, flag_file, flag_log, retainedTraceFilesDays)
                    outputTraces                      = getParameterFromFile(firstWord, '-to', flagValue, flag_file, flag_log, outputTraces)
                    outputRemovedTraces               = getParameterFromFile(firstWord, '-td', flagValue, flag_file, flag_log, outputRemovedTraces)
                    retainedDumpDays                  = getParameterFromFile(firstWord, '-dr', flagValue, flag_file, flag_log, retainedDumpDays)
                    retainedAnyFileDays               = getParameterFromFile(firstWord, '-gr', flagValue, flag_file, flag_log, retainedAnyFileDays)
                    anyFilePaths                      = getParameterListFromFile(firstWord, '-gd', flagValue, flag_file, flag_log, anyFilePaths)
                    anyFilePaths                      = [p.replace('%SID', SID) for p in anyFilePaths]
                    anyFileWords                      = getParameterListFromFile(firstWord, '-gw', flagValue, flag_file, flag_log, anyFileWords)
                    anyFileMaxDepth                   = getParameterFromFile(firstWord, '-gm', flagValue, flag_file, flag_log, anyFileMaxDepth)
                    zipBackupLogsSizeLimit            = getParameterFromFile(firstWord, '-zb', flagValue, flag_file, flag_log, zipBackupLogsSizeLimit)
                    zipBackupPath                     = getParameterFromFile(firstWord, '-zp', flagValue, flag_file, flag_log, zipBackupPath)
                    zipLinks                          = getParameterFromFile(firstWord, '-zl', flagValue, flag_file, flag_log, zipLinks)
                    zipOut                            = getParameterFromFile(firstWord, '-zo', flagValue, flag_file, flag_log, zipOut)
                    zipKeep                           = getParameterFromFile(firstWord, '-zk', flagValue, flag_file, flag_log, zipKeep)
                    minRetainedAlertDays              = getParameterFromFile(firstWord, '-ar', flagValue, flag_file, flag_log, minRetainedAlertDays)
                    minRetainedObjLockDays            = getParameterFromFile(firstWord, '-kr', flagValue, flag_file, flag_log, minRetainedObjLockDays)
                    outputAlerts                      = getParameterFromFile(firstWord, '-ao', flagValue, flag_file, flag_log, outputAlerts)
                    outputDeletedAlerts               = getParameterFromFile(firstWord, '-ad', flagValue, flag_file, flag_log, outputDeletedAlerts)
                    objHistMaxSize                    = getParameterFromFile(firstWord, '-om', flagValue, flag_file, flag_log, objHistMaxSize)
                    outputObjHist                     = getParameterFromFile(firstWord, '-oo', flagValue, flag_file, flag_log, outputObjHist)
                    maxFreeLogsegments                = getParameterFromFile(firstWord, '-lr', flagValue, flag_file, flag_log, maxFreeLogsegments)
                    minRetainedDaysForHandledEvents   = getParameterFromFile(firstWord, '-eh', flagValue, flag_file, flag_log, minRetainedDaysForHandledEvents)
                    minRetainedDaysForEvents          = getParameterFromFile(firstWord, '-eu', flagValue, flag_file, flag_log, minRetainedDaysForEvents)
                    retainedAuditLogDays              = getParameterFromFile(firstWord, '-ur', flagValue, flag_file, flag_log, retainedAuditLogDays)
                    pendingEmailsDays                 = getParameterFromFile(firstWord, '-pe', flagValue, flag_file, flag_log, pendingEmailsDays)
                    fragmentationLimit                = getParameterFromFile(firstWord, '-fl', flagValue, flag_file, flag_log, fragmentationLimit)
                    outputFragmentation               = getParameterFromFile(firstWord, '-fo', flagValue, flag_file, flag_log, outputFragmentation)
                    rcContainers                      = getParameterFromFile(firstWord, '-rc', flagValue, flag_file, flag_log, rcContainers)
                    outputRcContainers                = getParameterFromFile(firstWord, '-ro', flagValue, flag_file, flag_log, outputRcContainers)
                    maxRawComp                        = getParameterFromFile(firstWord, '-cc', flagValue, flag_file, flag_log, maxRawComp)
                    maxEstComp                        = getParameterFromFile(firstWord, '-ce', flagValue, flag_file, flag_log, maxEstComp)
                    maxRowComp                        = getParameterFromFile(firstWord, '-cr', flagValue, flag_file, flag_log, maxRowComp)
                    maxMemComp                        = getParameterFromFile(firstWord, '-cs', flagValue, flag_file, flag_log, maxMemComp)
                    minDistComp                       = getParameterFromFile(firstWord, '-cd', flagValue, flag_file, flag_log, minDistComp)
                    maxQuotaComp                      = getParameterFromFile(firstWord, '-cq', flagValue, flag_file, flag_log, maxQuotaComp)
                    maxUDIVComp                       = getParameterFromFile(firstWord, '-cu', flagValue, flag_file, flag_log, maxUDIVComp)
                    maxBLOCKComp                      = getParameterFromFile(firstWord, '-cb', flagValue, flag_file, flag_log, maxBLOCKComp)
                    partComp                          = getParameterFromFile(firstWord, '-cp', flagValue, flag_file, flag_log, partComp)
                    mergeBeforeComp                   = getParameterFromFile(firstWord, '-cm', flagValue, flag_file, flag_log, mergeBeforeComp)
                    outComp                           = getParameterFromFile(firstWord, '-co', flagValue, flag_file, flag_log, outComp)
                    createVTStat                      = getParameterFromFile(firstWord, '-vs', flagValue, flag_file, flag_log, createVTStat)
                    defaultVTStatType                 = getParameterFromFile(firstWord, '-vt', flagValue, flag_file, flag_log, defaultVTStatType)
                    maxRowsForDefaultVT               = getParameterFromFile(firstWord, '-vn', flagValue, flag_file, flag_log, maxRowsForDefaultVT)
                    largeVTStatType                   = getParameterFromFile(firstWord, '-vtt', flagValue, flag_file, flag_log, largeVTStatType)
                    otherDBVTStatType                 = getParameterFromFile(firstWord, '-vto', flagValue, flag_file, flag_log, otherDBVTStatType)
                    vtSchemas                         = getParameterListFromFile(firstWord, '-vl', flagValue, flag_file, flag_log, vtSchemas)
                    ignore2ndMon                      = getParameterFromFile(firstWord, '-vr', flagValue, flag_file, flag_log, ignore2ndMon)
                    refreshAge                        = getParameterFromFile(firstWord, '-vnr', flagValue, flag_file, flag_log, refreshAge)
                    minRetainedIniDays                = getParameterFromFile(firstWord, '-ir', flagValue, flag_file, flag_log, minRetainedIniDays)
                    execute_sql                       = getParameterFromFile(firstWord, '-es', flagValue, flag_file, flag_log, execute_sql)
                    out_sql                           = getParameterFromFile(firstWord, '-os', flagValue, flag_file, flag_log, out_sql)
                    out_path                          = getParameterFromFile(firstWord, '-op', flagValue, flag_file, flag_log, out_path)
                    out_prefix                        = getParameterFromFile(firstWord, '-of', flagValue, flag_file, flag_log, out_prefix)
                    minRetainedOutputDays             = getParameterFromFile(firstWord, '-or', flagValue, flag_file, flag_log, minRetainedOutputDays)
                    out_config                        = getParameterFromFile(firstWord, '-oc', flagValue, flag_file, flag_log, out_config)
                    online_test_interval              = getParameterFromFile(firstWord, '-oi', flagValue, flag_file, flag_log, online_test_interval)
                    file_system                       = getParameterFromFile(firstWord, '-fs', flagValue, flag_file, flag_log, file_system)
                    ignore_filesystems                = getParameterListFromFile(firstWord, '-if', flagValue, flag_file, flag_log, ignore_filesystems)
                    do_df_check                       = getParameterFromFile(firstWord, '-df', flagValue, flag_file, flag_log, do_df_check)
                    hanacleaner_interval              = getParameterFromFile(firstWord, '-hci', flagValue, flag_file, flag_log, hanacleaner_interval)
                    std_out                           = getParameterFromFile(firstWord, '-so', flagValue, flag_file, flag_log, std_out)
                    ssl                               = getParameterFromFile(firstWord, '-ssl', flagValue, flag_file, flag_log, ssl)
                    virtual_local_host                = getParameterFromFile(firstWord, '-vlh', flagValue, flag_file, flag_log, virtual_local_host)
                    dbuserkeys                        = getParameterListFromFile(firstWord, '-k', flagValue, flag_file, flag_log, dbuserkeys)
                    dbases                            = getParameterListFromFile(firstWord, '-dbs', flagValue, flag_file, flag_log, dbases)
                    receiver_emails                   = getParameterListFromFile(firstWord, '-en', flagValue, flag_file, flag_log, receiver_emails)
                    email_timeout                     = getParameterFromFile(firstWord, '-et', flagValue, flag_file, flag_log, email_timeout)
                    sendEmailSummary                  = getParameterFromFile(firstWord, '-ena', flagValue, flag_file, flag_log, sendEmailSummary)
                    email_client                      = getParameterFromFile(firstWord, '-enc', flagValue, flag_file, flag_log, email_client)
                    senders_email                     = getParameterFromFile(firstWord, '-ens', flagValue, flag_file, flag_log, senders_email)
                    mail_server                       = getParameterFromFile(firstWord, '-enm', flagValue, flag_file, flag_log, mail_server)

    #####################   INPUT ARGUMENTS (these would overwrite whats in the configuration file(s))   ####################
    for word in sys.argv:
        if word[0:1] == '-':
            checkIfAcceptedFlag(word)
    minRetainedBackups                = getParameterFromCommandLine(sys.argv, '-be', flag_log, minRetainedBackups)
    minRetainedDays                   = getParameterFromCommandLine(sys.argv, '-bd', flag_log, minRetainedDays)
    deleteBackups                     = getParameterFromCommandLine(sys.argv, '-bb', flag_log,  deleteBackups)
    outputCatalog                     = getParameterFromCommandLine(sys.argv, '-bo', flag_log, outputCatalog)
    outputDeletedCatalog              = getParameterFromCommandLine(sys.argv, '-br', flag_log, outputDeletedCatalog)
    outputNDeletedLBEntries           = getParameterFromCommandLine(sys.argv, '-bn', flag_log, outputNDeletedLBEntries)
    retainedTraceContentDays          = getParameterFromCommandLine(sys.argv, '-tc', flag_log, retainedTraceContentDays)
    retainedExpensiveTraceContentDays = getParameterFromCommandLine(sys.argv, '-te', flag_log, retainedExpensiveTraceContentDays)
    backupTraceContent                = getParameterFromCommandLine(sys.argv, '-tcb', flag_log, backupTraceContent)
    backupTraceDirectory              = getParameterFromCommandLine(sys.argv, '-tbd', flag_log, backupTraceDirectory)
    timeOutForMove                    = getParameterFromCommandLine(sys.argv, '-tmo', flag_log, timeOutForMove)
    retainedTraceFilesDays            = getParameterFromCommandLine(sys.argv, '-tf', flag_log, retainedTraceFilesDays)
    outputTraces                      = getParameterFromCommandLine(sys.argv, '-to', flag_log, outputTraces)
    outputRemovedTraces               = getParameterFromCommandLine(sys.argv, '-td', flag_log, outputRemovedTraces)
    retainedDumpDays                  = getParameterFromCommandLine(sys.argv, '-dr', flag_log, retainedDumpDays)
    retainedAnyFileDays               = getParameterFromCommandLine(sys.argv, '-gr', flag_log, retainedAnyFileDays)
    anyFilePaths                      = getParameterListFromCommandLine(sys.argv, '-gd', flag_log, anyFilePaths)
    anyFilePaths                      = [p.replace('%SID', SID) for p in anyFilePaths]
    anyFileWords                      = getParameterListFromCommandLine(sys.argv, '-gw', flag_log, anyFileWords)
    anyFileMaxDepth                   = getParameterFromCommandLine(sys.argv, '-gm', flag_log, anyFileMaxDepth)
    zipBackupLogsSizeLimit            = getParameterFromCommandLine(sys.argv, '-zb', flag_log, zipBackupLogsSizeLimit)
    zipBackupPath                     = getParameterFromCommandLine(sys.argv, '-zp', flag_log, zipBackupPath)
    zipLinks                          = getParameterFromCommandLine(sys.argv, '-zl', flag_log, zipLinks)
    zipOut                            = getParameterFromCommandLine(sys.argv, '-zo', flag_log, zipOut)
    zipKeep                           = getParameterFromCommandLine(sys.argv, '-zk', flag_log, zipKeep)
    minRetainedAlertDays              = getParameterFromCommandLine(sys.argv, '-ar', flag_log, minRetainedAlertDays)
    minRetainedObjLockDays            = getParameterFromCommandLine(sys.argv, '-kr', flag_log, minRetainedObjLockDays)
    outputAlerts                      = getParameterFromCommandLine(sys.argv, '-ao', flag_log, outputAlerts)
    outputDeletedAlerts               = getParameterFromCommandLine(sys.argv, '-ad', flag_log, outputDeletedAlerts)
    objHistMaxSize                    = getParameterFromCommandLine(sys.argv, '-om', flag_log, objHistMaxSize)
    outputObjHist                     = getParameterFromCommandLine(sys.argv, '-oo', flag_log, outputObjHist)
    maxFreeLogsegments                = getParameterFromCommandLine(sys.argv, '-lr', flag_log, maxFreeLogsegments)
    minRetainedDaysForHandledEvents   = getParameterFromCommandLine(sys.argv, '-eh', flag_log, minRetainedDaysForHandledEvents)
    minRetainedDaysForEvents          = getParameterFromCommandLine(sys.argv, '-eu', flag_log, minRetainedDaysForEvents)
    retainedAuditLogDays              = getParameterFromCommandLine(sys.argv, '-ur', flag_log, retainedAuditLogDays)
    pendingEmailsDays                 = getParameterFromCommandLine(sys.argv, '-pe', flag_log, pendingEmailsDays)
    fragmentationLimit                = getParameterFromCommandLine(sys.argv, '-fl', flag_log, fragmentationLimit)
    outputFragmentation               = getParameterFromCommandLine(sys.argv, '-fo', flag_log, outputFragmentation)
    rcContainers                      = getParameterFromCommandLine(sys.argv, '-rc', flag_log, rcContainers)
    outputRcContainers                = getParameterFromCommandLine(sys.argv, '-ro', flag_log, outputRcContainers)
    maxRawComp                        = getParameterFromCommandLine(sys.argv, '-cc', flag_log, maxRawComp)
    maxEstComp                        = getParameterFromCommandLine(sys.argv, '-ce', flag_log, maxEstComp)
    maxRowComp                        = getParameterFromCommandLine(sys.argv, '-cr', flag_log, maxRowComp)
    maxMemComp                        = getParameterFromCommandLine(sys.argv, '-cs', flag_log, maxMemComp)
    minDistComp                       = getParameterFromCommandLine(sys.argv, '-cd', flag_log, minDistComp)
    maxQuotaComp                      = getParameterFromCommandLine(sys.argv, '-cq', flag_log, maxQuotaComp)
    maxUDIVComp                       = getParameterFromCommandLine(sys.argv, '-cu', flag_log, maxUDIVComp)
    maxBLOCKComp                      = getParameterFromCommandLine(sys.argv, '-cb', flag_log, maxBLOCKComp)
    partComp                          = getParameterFromCommandLine(sys.argv, '-cp', flag_log, partComp)
    mergeBeforeComp                   = getParameterFromCommandLine(sys.argv, '-cm', flag_log, mergeBeforeComp)
    outComp                           = getParameterFromCommandLine(sys.argv, '-co', flag_log, outComp)
    createVTStat                      = getParameterFromCommandLine(sys.argv, '-vs', flag_log, createVTStat)
    defaultVTStatType                 = getParameterFromCommandLine(sys.argv, '-vt', flag_log, defaultVTStatType)
    maxRowsForDefaultVT               = getParameterFromCommandLine(sys.argv, '-vn', flag_log, maxRowsForDefaultVT)
    largeVTStatType                   = getParameterFromCommandLine(sys.argv, '-vtt', flag_log, largeVTStatType)
    otherDBVTStatType                 = getParameterFromCommandLine(sys.argv, '-vto', flag_log, otherDBVTStatType)
    vtSchemas                         = getParameterListFromCommandLine(sys.argv, '-vl', flag_log, vtSchemas)
    ignore2ndMon                      = getParameterFromCommandLine(sys.argv, '-vr', flag_log, ignore2ndMon)
    refreshAge                        = getParameterFromCommandLine(sys.argv, '-vnr', flag_log, refreshAge)
    minRetainedIniDays                = getParameterFromCommandLine(sys.argv, '-ir', flag_log, minRetainedIniDays)
    execute_sql                       = getParameterFromCommandLine(sys.argv, '-es', flag_log, execute_sql)
    out_sql                           = getParameterFromCommandLine(sys.argv, '-os', flag_log, out_sql)
    out_path                          = getParameterFromCommandLine(sys.argv, '-op', flag_log, out_path)
    out_prefix                        = getParameterFromCommandLine(sys.argv, '-of', flag_log, out_prefix)
    minRetainedOutputDays             = getParameterFromCommandLine(sys.argv, '-or', flag_log, minRetainedOutputDays)
    out_config                        = getParameterFromCommandLine(sys.argv, '-oc', flag_log, out_config)
    online_test_interval              = getParameterFromCommandLine(sys.argv, '-oi', flag_log, online_test_interval)
    file_system                       = getParameterFromCommandLine(sys.argv, '-fs', flag_log, file_system)
    ignore_filesystems                = getParameterListFromCommandLine(sys.argv, '-if', flag_log, ignore_filesystems)
    do_df_check                       = getParameterFromCommandLine(sys.argv, '-df', flag_log, do_df_check)
    hanacleaner_interval              = getParameterFromCommandLine(sys.argv, '-hci', flag_log, hanacleaner_interval)
    std_out                           = getParameterFromCommandLine(sys.argv, '-so', flag_log, std_out)
    ssl                               = getParameterFromCommandLine(sys.argv, '-ssl', flag_log, ssl)
    virtual_local_host                = getParameterFromCommandLine(sys.argv, '-vlh', flag_log, virtual_local_host)
    dbuserkeys                        = getParameterListFromCommandLine(sys.argv, '-k', flag_log, dbuserkeys)
    dbases                            = getParameterListFromCommandLine(sys.argv, '-dbs', flag_log, dbases)
    receiver_emails                   = getParameterListFromCommandLine(sys.argv, '-en', flag_log, receiver_emails)
    email_timeout                     = getParameterFromCommandLine(sys.argv, '-et', flag_log, email_timeout)
    sendEmailSummary                  = getParameterFromCommandLine(sys.argv, '-ena', flag_log, sendEmailSummary)
    email_client                      = getParameterFromCommandLine(sys.argv, '-enc', flag_log, email_client)
    senders_email                     = getParameterFromCommandLine(sys.argv, '-ens', flag_log, senders_email)
    mail_server                       = getParameterFromCommandLine(sys.argv, '-enm', flag_log, mail_server)

    ############ GET LOCAL HOST ##########
    #local_host = subprocess.check_output("hostname", shell=True).replace('\n','') if virtual_local_host == "" else virtual_local_host 
    local_host = run_command("hostname").replace('\n','') if virtual_local_host == "" else virtual_local_host   
    if not is_integer(local_host.split('.')[0]):    #first check that it is not an IP address
        local_host = local_host.split('.')[0]  #if full host name is specified in the local host (or virtual host), only the first part is used

    ############ CHECK EMAIL FLAGS #######
    ### receiver_emails, -en
    if receiver_emails:
        if any(not is_email(element) for element in receiver_emails):
            print("INPUT ERROR: some element(s) of -en is/are not email(s). Please see --help for more information.")
            os._exit(1)
    ### email_timeout, -et
    if not is_integer(email_timeout):
        print("INPUT ERROR: -et must be an integer. Please see --help for more information.")
        os._exit(1)
    email_timeout = int(email_timeout)
    if email_timeout >= 0 and not receiver_emails:
        print("INPUT ERROR: -et is specified although -en is not, this makes no sense. Please see --help for more information.")
        os._exit(1)
    ### sendEmailSummary, -ena
    sendEmailSummary = checkAndConvertBooleanFlag(sendEmailSummary, "-ena")
    if sendEmailSummary:
        if not receiver_emails:
            print("INPUT ERROR: -ena is true although -en is not specified, this makes no sense. Please see --help for more information.")
            os._exit(1)
    ### email_client, -enc
    if email_client:
        if not receiver_emails:
            print("INPUT ERROR: -enc is specified although -en is not, this makes no sense. Please see --help for more information.")
            os._exit(1)
    if receiver_emails:
        if not email_client:
            email_client = 'mailx'
        if email_client not in ['mailx', 'mail', 'mutt']:
            print("INPUT ERROR: The -enc flag does not specify any of the email clients mailx, mail, or mutt. If you are using another email client that can send emails with the command ")
            print('             <message> | <client> -s "<subject>" \n please let me know.')
            os._exit(1)
    ### senders_email, -ens
    if senders_email:
        if not receiver_emails:
            print("INPUT ERROR: -ens is specified although -en is not, this makes no sense. Please see --help for more information.")
            os._exit(1)
        if not is_email(senders_email):
            print("INPUT ERROR: -ens is not an email. Please see --help for more information.")
            os._exit(1)
    ### mail_server, -enm
    if mail_server:
        if not receiver_emails:
            print("INPUT ERROR: -enm is specified although -en is not, this makes no sense. Please see --help for more information.")
            os._exit(1)
    emailSender = None
    if receiver_emails:
        emailSender = EmailSender(receiver_emails, email_client, senders_email, mail_server, SID)

    ############# STD OUT, LOG DIRECTORY and LOG MANAGER #########
    std_out = checkAndConvertBooleanFlag(std_out, "-so", LogManager("", "", True, emailSender))
    log_path = out_path.replace(" ","_").replace(".","_")
    log_path = log_path.replace('%SID', SID)     
    if log_path and not os.path.exists(log_path):
        os.makedirs(log_path)
    logman = LogManager(log_path, out_prefix, std_out, emailSender)

    ############ CHECK FOR DISK FULL SITUATION ###################
    ### do_df_check, -df
    do_df_check = checkAndConvertBooleanFlag(do_df_check, "-df", logman)
    if do_df_check:
        if max_filesystem_usage_in_percent(file_system, ignore_filesystems, logman) > 98:
            log('ERROR: HANACleaner is not supported during a "disk full situation". Currently one of your filesystem is using more than 98% of available disk space. Please solve this issue and then run HANACleaner again.', logman, True)
            os._exit(1)

    ############ CHECK AND CONVERT INPUT PARAMETERS #################
    #log("\nHANACleaner executed "+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" with \n"+" ".join(sys.argv)+"\n", logman)
    ### ssl, -ssl
    ssl = checkAndConvertBooleanFlag(ssl, "-ssl", logman)
    hdbsql_string = "hdbsql "
    if ssl:
        hdbsql_string = "hdbsql -e -ssltrustcert -sslcreatecert "        
    ### minRetainedBackups, -be 
    if not is_integer(minRetainedBackups):
        log("INPUT ERROR: -be must be an integer. Please see --help for more information.", logman)
        os._exit(1)
    minRetainedBackups = int(minRetainedBackups)
    if minRetainedBackups == 0:
        log("INPUT ERROR: -be is not allowed to be 0, we must keep at least one data backup entry. Please see --help for more information.", logman, True)
        os._exit(1)
    if minRetainedBackups < 0:
        minRetainedBackups = -1
    ### minRetainedDays, -bd
    if not is_integer(minRetainedDays):
        log("INPUT ERROR: -bd must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedDays = int(minRetainedDays)
    # deleteBackups, -bb
    deleteBackups = checkAndConvertBooleanFlag(deleteBackups, "-bb", logman)    
    if deleteBackups and (minRetainedBackups < 0 and minRetainedDays < 0):
        log("INPUT ERROR: If -bb is 'true' then -be and -bd cannot both be '-1'. Please see --help for more information.", logman, True)
        os._exit(1)
    ### outputCatalog, -bo
    outputCatalog = checkAndConvertBooleanFlag(outputCatalog, "-bo", logman)
    ### outputDeletedCatalog, -br
    outputDeletedCatalog = checkAndConvertBooleanFlag(outputDeletedCatalog, "-br", logman)
    ### outputNDeletedLBEntries, -bn
    outputNDeletedLBEntries = checkAndConvertBooleanFlag(outputNDeletedLBEntries, "-bn", logman)
    ### outputTraces, -to
    outputTraces = checkAndConvertBooleanFlag(outputTraces, "-to", logman)
    ### outputRemovedTraces, -td
    outputRemovedTraces = checkAndConvertBooleanFlag(outputRemovedTraces, "-td", logman)
    ### retainedTraceContentDays, -tc
    if not is_integer(retainedTraceContentDays):
        log("INPUT ERROR: -tc must be an integer. Please see --help for more information.", logman)
        os._exit(1)
    ### retainedExpensiveTraceContentDays, -te
    if not is_integer(retainedExpensiveTraceContentDays):
        log("INPUT ERROR: -te must be an integer. Please see --help for more information.", logman)
        os._exit(1)
    ### backupTraceContent, -tcb
    backupTraceContent = checkAndConvertBooleanFlag(backupTraceContent, "-tcb", logman)
    if backupTraceContent and (retainedTraceContentDays == "-1" and retainedExpensiveTraceContentDays == "-1"):
        log("INPUT ERROR: -tcb is specified although -tc and -te are not. This makes no sense. Please see --help for more information.", logman, True)
        os._exit(1)
    ### backupTraceDirectory, -tbd
    if backupTraceDirectory and not backupTraceContent:
        log("INPUT ERROR: -tbd is specified although -tcb is not. This makes no sense. Please see --help for more information.", logman, True)
        os._exit(1)
    ### timeOutForMove, -tmo
    if not is_integer(timeOutForMove):
        log("INPUT ERROR: -tmo must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    timeOutForMove = int(timeOutForMove)
    ### retainedTraceFilesDays, -tf
    if not is_integer(retainedTraceFilesDays):
        log("INPUT ERROR: -tf must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    if outputRemovedTraces:
        if retainedTraceContentDays == "-1" and retainedExpensiveTraceContentDays == "-1" and retainedTraceFilesDays == "-1":
            log("INPUT ERROR: -td is true allthough -tc, -te and -tf are all -1. This makes no sense. Please see --help for more information.", logman, True)
            os._exit(1)
    ### retainedDumpDays, -dr
    if not is_integer(retainedDumpDays):
        log("INPUT ERROR: -dr must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    ### retainedAnyFileDays, -gr
    if not is_integer(retainedAnyFileDays):
        log("INPUT ERROR: -gr must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    ### anyFilePaths, -gd
    if anyFilePaths[0]:
        if not all(os.path.isdir(path) for path in anyFilePaths):
            log("INPUT ERROR: -gd must be a directory or a list of directories. Please see --help for more information.", logman, True)
            os._exit(1)
    ### anyFileWords, -gw
    if not len(anyFileWords) == len(anyFilePaths):
        log("INPUT ERROR: -gw must be a list of the same length as -gd. Please see --help for more information.", logman, True)
        os._exit(1)
    if len(anyFileWords) == 1 and anyFileWords[0] == "" and not retainedAnyFileDays == "-1":
        log("INPUT ERROR: -gw must be specified if -gr is. Please see --help for more information.", logman, True)
        os._exit(1)
    ### anyFileMaxDepth, -gm
    if not is_integer(anyFileMaxDepth):
        log("INPUT ERROR: -gm must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    anyFileMaxDepth = int(anyFileMaxDepth)
    if anyFileMaxDepth < 1 or anyFileMaxDepth > 10:
        log("INPUT ERROR: -gm must be between 1 and 10. Please see --help for more information.", logman, True)
        os._exit(1)
    ### zipBackupLogsSizeLimit, -zb
    if not is_integer(zipBackupLogsSizeLimit):
        log("INPUT ERROR: -zb must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)       
    zipBackupLogsSizeLimit = int(zipBackupLogsSizeLimit)
    if zipBackupLogsSizeLimit != -1:
        ### zipBackupPath, -zp
        if zipBackupPath: #default has been put to '' and will be put to cdtrace as soon as we get local_instance
            if not os.path.exists(zipBackupPath):
                log("INPUT ERROR: The path provided with -zp does not exist. Please see --help for more information.\n"+zipBackupPath, logman, True)
                os._exit(1)
    ### zipLinks, -zl
    zipLinks = checkAndConvertBooleanFlag(zipLinks, "-zl", logman)
    ### zipOut, -zo
    zipOut = checkAndConvertBooleanFlag(zipOut, "-zo", logman)
    ### zipKeep, -zk
    zipKeep = checkAndConvertBooleanFlag(zipKeep, "-zk", logman)
    ### minRetainedAlertDays, -ar
    if not is_integer(minRetainedAlertDays):
        log("INPUT ERROR: -ar must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedAlertDays = int(minRetainedAlertDays)
    ### minRetainedObjLockDays, -kr
    if not is_integer(minRetainedObjLockDays):
        log("INPUT ERROR: -kr must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedObjLockDays = int(minRetainedObjLockDays)
    ### outputAlerts, -ao
    outputAlerts = checkAndConvertBooleanFlag(outputAlerts, "-ao", logman)
    ### outputDeletedAlerts, -ad
    outputDeletedAlerts = checkAndConvertBooleanFlag(outputDeletedAlerts, "-ad", logman)
    ### objHistMaxSize, -om
    if not is_integer(objHistMaxSize):
        log("INPUT ERROR: -om must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)    
    objHistMaxSize = int(objHistMaxSize)
    ### outputObjHist, -oo
    outputObjHist = checkAndConvertBooleanFlag(outputObjHist, "-oo", logman)    
    ### maxFreeLogsegments, -lr 
    if not is_integer(maxFreeLogsegments):
        log("INPUT ERROR: -lr must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxFreeLogsegments = int(maxFreeLogsegments)
    ### minRetainedDaysForHandledEvents, -eh
    if not is_integer(minRetainedDaysForHandledEvents):
        log("INPUT ERROR: -eh must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedDaysForHandledEvents = int(minRetainedDaysForHandledEvents) 
    ### minRetainedDaysForEvents, -eu
    if not is_integer(minRetainedDaysForEvents):
        log("INPUT ERROR: -eu must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedDaysForEvents = int(minRetainedDaysForEvents)
    if minRetainedDaysForHandledEvents >= 0 and minRetainedDaysForEvents >= 0 and minRetainedDaysForHandledEvents > minRetainedDaysForEvents:
        log("INPUT ERROR: it does not make sense that -eh > -eu. Please see --help for more information.", logman, True)
        os._exit(1)
    ### retainedAuditLogDays, -ur
    if not is_integer(retainedAuditLogDays):
        log("INPUT ERROR: -ur must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)    
    ### pendingEmailsDays, -pe
    if not is_integer(pendingEmailsDays):
        log("INPUT ERROR: -pe must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    ### fragmentationLimit, -fl
    if not is_integer(fragmentationLimit):
        log("INPUT ERROR: -fl must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    fragmentationLimit = int(fragmentationLimit)
    ### outputFragmentation, -fo
    outputFragmentation = checkAndConvertBooleanFlag(outputFragmentation, "-fo", logman)
    ### rcContainers, -rc
    rcContainers = checkAndConvertBooleanFlag(rcContainers, "-rc", logman)
    ### outputRcContainers, -ro
    outputRcContainers = checkAndConvertBooleanFlag(outputRcContainers, "-ro", logman)
    ### maxRawComp, -cc
    if not is_integer(maxRawComp):
        log("INPUT ERROR: -cc must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxRawComp = int(maxRawComp)
    ### maxEstComp, -ce
    if not is_integer(maxEstComp):
        log("INPUT ERROR: -ce must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxEstComp = int(maxEstComp)
    ### maxRowComp, -cr
    if not is_integer(maxRowComp):
        log("INPUT ERROR: -cr must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxRowComp = int(maxRowComp)
    ### maxMemComp, -cs
    if not is_integer(maxMemComp):
        log("INPUT ERROR: -cs must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxMemComp = int(maxMemComp)
    ### minDistComp, -cd
    if not is_integer(minDistComp):
        log("INPUT ERROR: -cd must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minDistComp = int(minDistComp)
    ### maxQuotaComp, -cq
    if not is_integer(maxQuotaComp):
        log("INPUT ERROR: -cq must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxQuotaComp = int(maxQuotaComp)    
    ### maxUDIVComp, -cu
    if not is_integer(maxUDIVComp):
        log("INPUT ERROR: -cu must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxUDIVComp = int(maxUDIVComp)
    ### maxBLOCKComp, -cb
    if not is_integer(maxBLOCKComp):
        log("INPUT ERROR: -cb must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxBLOCKComp = int(maxBLOCKComp)
    ### partComp, -cp
    partComp = checkAndConvertBooleanFlag(partComp, "-cp", logman)
    ### mergeBeforeComp, -cm
    mergeBeforeComp = checkAndConvertBooleanFlag(mergeBeforeComp, "-cm", logman)
    ### outComp, -co
    outComp = checkAndConvertBooleanFlag(outComp, "-co", logman)
    ### createVTStat, -vs
    createVTStat = checkAndConvertBooleanFlag(createVTStat, "-vs", logman)
    ### defaultVTStatType, -vt
    if defaultVTStatType not in ['HISTOGRAM', 'SIMPLE', 'TOPK', 'SKETCH', 'SAMPLE', 'RECORD_COUNT']:
        log("INPUT ERROR: Wrong input option of -vt. Please see --help for more information.", logman, True)
        os._exit(1)
    if defaultVTStatType == 'RECORD_COUNT':
        defaultVTStatType = 'RECORD COUNT'
    ### maxRowsForDefaultVT, -vn
    if not is_integer(maxRowsForDefaultVT):
        log("INPUT ERROR: -vn must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    maxRowsForDefaultVT = int(maxRowsForDefaultVT)
    ### largeVTStatType, -vtt
    if largeVTStatType not in ['HISTOGRAM', 'SIMPLE', 'TOPK', 'SKETCH', 'SAMPLE', 'RECORD_COUNT']:
        log("INPUT ERROR: Wrong input option of -vtt. Please see --help for more information.", logman, True)
        os._exit(1)
    if largeVTStatType == 'RECORD_COUNT':
        largeVTStatType = 'RECORD COUNT'
    ### otherDBVTStatType, -vto
    if otherDBVTStatType not in ['HISTOGRAM', 'SIMPLE', 'TOPK', 'SKETCH', 'SAMPLE', 'RECORD_COUNT', '']:
        log("INPUT ERROR: Wrong input option of -vto. Please see --help for more information.", logman, True)
        os._exit(1)
    if otherDBVTStatType == 'RECORD_COUNT':
        otherDBVTStatType = 'RECORD COUNT'
    ### vtSchemas, -vl
    #Nothing to check here, will check later if all schemas exist
    ### ignore2ndMon, -vr
    ignore2ndMon = checkAndConvertBooleanFlag(ignore2ndMon, "-vr", logman)
    ### refreshAge, -vnr
    if not is_integer(refreshAge):
        log("INPUT ERROR: -vnr must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    refreshAge = int(refreshAge)
    ### minRetainedIniDays, -ir
    if not is_integer(minRetainedIniDays):
        log("INPUT ERROR: -ir must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedIniDays = int(minRetainedIniDays)
    if minRetainedIniDays < 365 and minRetainedIniDays != -1:
        log("INPUT ERROR: -ir must be larger than 365. Please see --help for more information. (If you disagree please remove this check on your own risk.)", logman, True)
        os._exit(1)
    ### hanacleaner_interval, -hci
    if not is_integer(hanacleaner_interval):
        log("INPUT ERROR: -hci must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    hanacleaner_interval = int(hanacleaner_interval)*24*3600  # days to seconds
    ### execute_sql, -es
    execute_sql = checkAndConvertBooleanFlag(execute_sql, "-es", logman)
    ### out_sql, -os
    out_sql = checkAndConvertBooleanFlag(out_sql, "-os", logman)
    ### minRetainedOutputDays, -or
    if not is_integer(minRetainedOutputDays):
        log("INPUT ERROR: -or must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    minRetainedOutputDays = int(minRetainedOutputDays)
    if minRetainedOutputDays >= 0 and out_path == "":
        log("INPUT ERROR: -op has to be specified if -or is. Please see --help for more information.", logman, True)
        os._exit(1)
    ### out_config, -oc
    out_config = checkAndConvertBooleanFlag(out_config, "-oc", logman)
    ### online_test_interval, -oi
    if not is_integer(online_test_interval):
        log("INPUT ERROR: -oi must be an integer. Please see --help for more information.", logman, True)
        os._exit(1)
    online_test_interval = int(online_test_interval)
    ### dbases, -dbs, and dbuserkeys, -k
    if len(dbases) > 1 and len(dbuserkeys) > 1:
        log("INPUT ERROR: -k may only specify one key if -dbs is used. Please see --help for more information.", logman, True)
        os._exit(1)
    ### dbases, option with all data bases: -dbs all (-k must point to systemdb)
    if len(dbases) == 1 and len(dbuserkeys) == 1 and 'all' in dbases:
        dbuserkey = dbuserkeys[0]
        dbases = get_all_databases(execute_sql, hdbsql_string, dbuserkey, local_host, out_sql, logman)   

    ################ START #################
    while True: # hanacleaner intervall loop
        for dbuserkey in dbuserkeys:
            ################ SET TIMEOUT ALARM #############
            def timeout_handler(signum, frame):
                log('Warning: HANACleaner has been running longer than '+str(email_timeout)+' seconds.', logman, True)
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(email_timeout)
            ############ GET LOCAL INSTANCE and SID ##########
            [key_hosts, ENV, DATABASE] = get_key_info(dbuserkey, local_host, logman)
            local_host_index = key_hosts.index(local_host)
            key_sqlports = [env.split(':')[1] for env in ENV]        
            dbinstances = [port[1:3] for port in key_sqlports]
            if not all(x == dbinstances[0] for x in dbinstances):
                print("ERROR: The hosts provided with the user key, "+dbuserkey+", does not all have the same instance number")
                os._exit(1)
            local_dbinstance = dbinstances[local_host_index]
            ############# MULTIPLE DATABASES #######
            for dbase in dbases:   #if -dbs = dbases are specified, this overwrites DATABASE (that could come from the key)
                if dbase:
                    DATABASE = dbase
                emailmessage = ""
                ############# SQL MANAGER ##############
                sqlman = SQLManager(execute_sql, hdbsql_string, dbuserkey, DATABASE, out_sql)
                db_string = ''
                if DATABASE:
                    db_string = 'on DB '+DATABASE
                #whoami = subprocess.check_output('whoami', shell=True).replace('\n','')
                whoami = run_command('whoami').replace('\n','')
                parameter_string = ""
                if out_config:
                    parameter_string = "\n".join("{}\t{}".format(k, "= "+v[0]+" from "+v[1]) for k, v in flag_log.items())
                if sqlman.execute:
                    startstring = "***********************************************************\n"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"\nhanacleaner as "+whoami+" by "+dbuserkey+" on "+SID+"("+local_dbinstance+") "+db_string+" with \n"+" ".join(sys.argv)+"\nCleanup Statements will be executed (-es is default true)\n"+parameter_string+" Before using HANACleaner read the disclaimer!\n    python hanacleaner.py --disclaimer\n***********************************************************" 
                else:
                    startstring = "*********************************************\n"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"\nhanacleaner as "+whoami+" by "+dbuserkey+"\non "+SID+"("+local_dbinstance+") "+db_string+" with \n"+" ".join(sys.argv)+"\nCleanup Statements will NOT be executed\n"+parameter_string+" Before using HANACleaner read the disclaimer!\n    python hanacleaner.py --disclaimer\n*********************************************"
                log(startstring, logman)
                emailmessage += startstring+"\n"
                ############ ONLINE TESTS (OPTIONAL) ##########################
                while not online_and_master_tests(online_test_interval, local_dbinstance, local_host, logman):  #will check if Online and if Primary and not Stand-By, and then if Master, but only if online_test_interval > -1           
                    log("\nOne of the online checks found out that this HANA instance, "+str(local_dbinstance)+", is not online or not master. ", logman)
                    ############ CLEANUP of OWN LOGS, HANACLEANER MUST DO even though HANA is OFFLINE ##########################
                    if minRetainedOutputDays >= 0:
                        nCleaned = clean_output(minRetainedOutputDays, sqlman, logman)
                        logmessage = str(nCleaned)+" hanacleaner daily log files were removed (-or) even though HANA is offline"
                        log(logmessage, logman)
                        emailmessage += logmessage+"\n"
                    else:
                        log("    (Cleaning of the hanacleaner logs was not done since -or was negative (or not specified))", logman)  
                    if online_test_interval == 0:
                        log("HANACleaner will now abort since online_test_interval = 0.", logman, True)
                        os._exit(1)
                    else:
                        log("HANACleaner will now have a "+str(online_test_interval)+" seconds break and check again if this Instance is online, or master, after the break.\n", logman)
                        time.sleep(float(online_test_interval))  # wait online_test_interval seconds before again checking if HANA is running
                ############ CHECK THAT USER CAN CONNECT TO HANA ###############  
                sql = "SELECT * from DUMMY" 
                errorlog = "USER ERROR: The user represented by the key "+dbuserkey+" cannot connect to the system. Make sure this user is properly saved in hdbuserstore."
                [dummy_out, succeeded] = try_execute_sql(sql, errorlog, sqlman, logman)
                dummy_out = dummy_out.strip("\n").strip("|").strip(" ") 
                if sqlman.execute and (dummy_out != 'X' or not succeeded):
                    log("USER ERROR: The user represented by the key "+dbuserkey+" cannot connect to the system. Make sure this user is properly saved in hdbuserstore.", logman, True)
                    os._exit(1)
                ##### HANA VERSION COMPATABILITY ######    
                [version, revision, mrevision] = hana_version_revision_maintenancerevision(sqlman, logman)
                if (retainedTraceContentDays != "-1" or retainedExpensiveTraceContentDays != "-1") and (version < 2 and revision < 120):
                    log("VERSION ERROR: -tc and -te are not supported for SAP HANA rev. < 120. (The UNTIL option is new with SPS12.)", logman, True)
                    os._exit(1)       
                if zipBackupLogsSizeLimit != -1 and (version >= 2 and revision >= 40):
                    log("VERSION WARNING: -zb is not supported for SAP HANA 2 rev. >= 40. Instead configure size with parameters, see SAP Note 2797078.", logman)
                    zipBackupLogsSizeLimit = -1     
                ###### START ALL HOUSE KEEPING TASKS ########
                if minRetainedBackups >= 0 or minRetainedDays >= 0:
                    [nCleanedData, nCleanedLog] = clean_backup_catalog(minRetainedBackups, minRetainedDays, deleteBackups, outputCatalog, outputDeletedCatalog, outputNDeletedLBEntries, sqlman, logman)
                    logmessage = str(nCleanedData)+" data backup entries and "+str(nCleanedLog)+" log backup entries were removed from the backup catalog (-be and -bd)"
                    if not outputNDeletedLBEntries:
                        logmessage = str(nCleanedData)+" data backup entries were removed from the backup catalog (number removed log backups is unknown since -bn = false)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of the backup catalog was not done since -be and -bd were both negative (or not specified))", logman)
                if retainedTraceContentDays != "-1" or retainedExpensiveTraceContentDays != "-1" or retainedTraceFilesDays != "-1":
                    nCleaned = clean_trace_files(retainedTraceContentDays, retainedExpensiveTraceContentDays, backupTraceContent, backupTraceDirectory, timeOutForMove, retainedTraceFilesDays, outputTraces, outputRemovedTraces, SID, DATABASE, local_dbinstance, hosts(sqlman), sqlman, logman)
                    logmessage = str(nCleaned)+" trace files were removed (-tc and -tf)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning traces was not done since -tc and -tf were both -1 (or not specified))", logman)
                if retainedDumpDays != "-1":
                    nCleaned = clean_dumps(retainedDumpDays, local_dbinstance, sqlman, logman)
                    logmessage = str(nCleaned)+" fullsysteminfodump zip files (that can contain both fullsystem dumps and runtime dumps) were removed (-dr)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning dumps was not done since -dr was -1 (or not specified))", logman)
                if retainedAnyFileDays != "-1":
                    nCleaned = clean_anyfile(retainedAnyFileDays, anyFilePaths, anyFileWords, anyFileMaxDepth, sqlman, logman)
                    logmessage = str(nCleaned)+" general files were removed (-gr)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of general files was not done since -gr was -1 (or not specified))", logman)
                if zipBackupLogsSizeLimit >= 0:
                    if not zipBackupPath:
                        zipBackupPath = cdalias('cdtrace', local_dbinstance)
                    nZipped = zipBackupLogs(zipBackupLogsSizeLimit, zipBackupPath, zipLinks, zipOut, zipKeep, sqlman, logman)
                    logmessage = str(nZipped)+" backup logs were compressed (-zb)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Compression of the backup logs was not done since -zb was negative (or not specified) or this is not more supported for your HANA Version)", logman)
                if minRetainedAlertDays >= 0:
                    nCleaned = clean_alerts(minRetainedAlertDays, outputAlerts, outputDeletedAlerts, sqlman, logman)
                    logmessage = str(nCleaned)+" alerts were removed (-ar)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of the alerts was not done since -ar was negative (or not specified))", logman)
                if minRetainedObjLockDays >= 0:
                    nCleaned = clean_objlock(minRetainedObjLockDays, sqlman, logman)
                    logmessage = str(nCleaned)+" object locks entries with unknown object names were removed (-kr)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of unknown object locks entries was not done since -kr was negative (or not specified))", logman)
                if objHistMaxSize >= 0:
                    memoryCleaned = clean_objhist(objHistMaxSize, outputObjHist, sqlman, logman)
                    logmessage = str(memoryCleaned)+" mb were cleaned from object history (-om)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of the object history was not done since -om was negative (or not specified))", logman)
                if maxFreeLogsegments >= 0:
                    nReclaimed = reclaim_logsegments(maxFreeLogsegments, sqlman, logman)
                    logmessage = str(nReclaimed)+" log segments were reclaimed (-lr)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Reclaim of free logsements was not done since -lr was negative (or not specified))", logman)
                if minRetainedDaysForHandledEvents >= 0 or minRetainedDaysForEvents >= 0:
                    nEventsCleaned = clean_events(minRetainedDaysForHandledEvents, minRetainedDaysForEvents, sqlman, logman)
                    logmessage = str(nEventsCleaned[1])+" events were cleaned, "+str(nEventsCleaned[0])+" of those were handled. There are "+str(nEventsCleaned[2])+" events left, "+str(nEventsCleaned[3])+" of those are handled. (-eh and -eu)" 
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of events was not done since -eh and -eu were negative (or not specified))", logman)
                if retainedAuditLogDays != "-1":
                    nCleaned = clean_audit_logs(retainedAuditLogDays, sqlman, logman)
                    logmessage = str(nCleaned)+" entries in the audit log table were removed (-ur)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning audit logs was not done since -ur was -1 (or not specified))", logman)  
                if pendingEmailsDays != "-1":
                    nCleaned = clean_pending_emails(pendingEmailsDays, sqlman, logman)
                    logmessage = str(nCleaned)+" pending statistics server email notifications were removed (-pe)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of pending emails was not done since -pe was -1 (or not specified))", logman)  
                if fragmentationLimit >= 0:
                    defragmentedPerPort = defragment(fragmentationLimit, outputFragmentation, sqlman, logman)
                    if defragmentedPerPort:
                        for port in defragmentedPerPort:
                            if port[2] > 0:
                                logmessage = "For Host "+str(port[0])+" and Port "+str(port[1])+" defragmentation changed by "+str(port[2])+" % (-fl)"
                                log(logmessage, logman)
                                emailmessage += logmessage+"\n"
                            else:
                                log("Defragmentation was tried for Host "+str(port[0])+" and Port "+str(port[1])+" but it changed by "+str(port[2])+" %", logman)
                    else:
                        log("Defragmentation was not done since there was not enough fragmentation for any service", logman)
                else:
                    log("    (Defragmentation was not done since -fl was negative (or not specified))", logman)
                if rcContainers:
                    nReclaimedContainers = reclaim_rs_containers(outputRcContainers, sqlman, logman)
                    logmessage = nReclaimedContainers[1]+" row store containers were reclaimed from "+nReclaimedContainers[0]+" row store tables (-rc)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Reclaim of row store containers was not done since -rc was negative (or not specified))", logman)
                if all(c > -1 for c in [maxRawComp, maxEstComp]) or all(c > -1 for c in [maxRowComp, maxMemComp, minDistComp]) or all(c > -1 for c in [maxQuotaComp, maxUDIVComp]) or maxBLOCKComp > -1:
                    nTablesForcedCompression = force_compression(maxRawComp, maxEstComp, maxRowComp, maxMemComp, minDistComp, maxQuotaComp, maxUDIVComp, maxBLOCKComp, partComp, mergeBeforeComp, version, revision, mrevision, outComp, sqlman, logman)
                    if nTablesForcedCompression[1]:
                        log("Tried re-optimize compression on "+str(nTablesForcedCompression[0])+" tables and failed on "+str(nTablesForcedCompression[1])+" (probably due to insufficient privileges)", logman)
                    else:
                        logmessage = str(nTablesForcedCompression[0])+" column store tables were compression re-optimized"
                        log(logmessage, logman)
                        emailmessage += logmessage+"\n"
                else:
                    log("    (Compression re-optimization was not done since at least one flag in each of the three compression flag groups was negative (or not specified))", logman)
                if createVTStat:
                    [nVTs, nVTsOptimized] = create_vt_statistics(vtSchemas, defaultVTStatType, maxRowsForDefaultVT, largeVTStatType, otherDBVTStatType, ignore2ndMon, sqlman, logman)
                    logmessage = "Optimization statistics was created for "+str(nVTsOptimized)+" virtual tables (in total there are "+str(nVTs)+" virtual tables) (-vs)" 
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Creation of optimization statistics for virtual tables was not done since -vs was false (or not specified))", logman)
                if refreshAge > 0:
                    [nDSs, nDSsRefreshed] = refresh_statistics(vtSchemas, refreshAge, ignore2ndMon, sqlman, logman)
                    logmessage = "Refresh of statistics was done for "+str(nDSsRefreshed)+" data statistics (in total there are "+str(nDSs)+" data statistics) (-vnr)" 
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Refresh of optimization statistics for virtual tables was not done since -vnr was not more than 0 (or not specified))", logman)
                if minRetainedIniDays >= 0:
                    nCleaned = clean_ini(minRetainedIniDays, version, revision, mrevision, sqlman, logman)
                    logmessage = str(nCleaned)+" inifile history contents were removed" 
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                if minRetainedOutputDays >= 0:
                    nCleaned = clean_output(minRetainedOutputDays, sqlman, logman)
                    logmessage = str(nCleaned)+" hanacleaner daily log files were removed (-or)"
                    log(logmessage, logman)
                    emailmessage += logmessage+"\n"
                else:
                    log("    (Cleaning of the hanacleaner logs was not done since -or was negative (or not specified))", logman)  
                ##### SEND EMAIL SUMMARY #####
                if sendEmailSummary:
                    sendEmail(emailmessage, logman)   
            ################ DISABLE TIMEOUT ALARM #############
            signal.alarm(0)

        # HANACLEANER INTERVALL
        if hanacleaner_interval < 0: 
            sys.exit()
        time.sleep(float(hanacleaner_interval))               
              
              
              
if __name__ == '__main__':
    main()
                        

