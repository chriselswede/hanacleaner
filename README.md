# SAP HANACleaner #
A house keeping script for SAP HANA
---

### DESCRIPTION:  
The SAP HANA cleaner is a house keeping script for SAP HANA. It can be used to clean the backup catalog, diagnostic files, alerts, to compress the backup logs, and much more. It should be executed by <sid>adm or, in case you use a CRON job, with the same environment as the <sid>adm. See SAP Note [2399996](https://launchpad.support.sap.com/#/notes/=2399996) and SAP Note [2400024](https://launchpad.support.sap.com/#/notes/=2400024). For a list of all input flags execute with  
   `python hanacleaner.py --help`



### DISCLAIMER:  
ANY USAGE OF HANACLEANER ASSUMES THAT YOU HAVE UNDERSTOOD AND AGREED THAT:
1. HANACleaner is NOT SAP official software, so normal SAP support of HANACleaner cannot be assumed 
2. HANACleaner is open source 
3. HANACleaner is provided "as is" 
4. HANACleaner is to be used on "your own risk" 
5. HANACleaner is a one-man's hobby; developed, maintained and supported only during non-working hours  
6. All HANACleaner documentations have to be read and understood before any usage:
* SAP Note [2399996](https://launchpad.support.sap.com/#/notes/2399996)
* The .pdf file hanacleaner_intro.pdf
* All output from executing    `python hanacleaner.py --help`  
7. HANACleaner can help you execute certain SAP HANA tasks automatically but is NOT an attempt to teach you SAP HANA  
   Therefore it is assumed that you understand all SQL statements that HANACleaner does to make changes in your system  
   To find out what crucial SQL statements HANACleaner will do without executing them, run with the additional flags  
           `-es false -os true`  
   To then learn what those statements do before you executing HANACleaner without "-es false", see SAP HANA Admin Guide or 
   SAP HANA System Administration Workshops 
8. HANACleaner is not providing any recommendations, all flags shown in the documentation (see point 6.) are only examples  
   For recommendations see SAP HANA Administration Workshops or other documentation, like e.g. SAP Note [2400024](https://launchpad.support.sap.com/#/notes/2400024)
