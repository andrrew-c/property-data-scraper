## Import modules
import requests 
from bs4 import BeautifulSoup as bs
import json
import sqlite3
import pickle
import datetime
import time
from datetime import datetime
import atexit

## Import system/operating system modules
import sys
import os

## Import own functions
import flats_tools as ft

import pdb

logging = False

## Suggested changes
print("{0}\nCreate some global error tracking thing for props which cannot be updated\n{0}".format("*"*100))

#####################################################
## Create log file
#####################################################

## Today in YYYYMMDD format
today = datetime.today().strftime('%Y%m%d')

## Path of current working directory (based on this file)
currentpath = os.path.dirname(os.path.abspath(__file__))
pcodespath = os.path.abspath("{}/master_postcode_lists".format(currentpath))
pcodesfile_ = "postcodes_list_master.csv"
pcodesfile = os.path.abspath("{}/{}".format(pcodespath, pcodesfile_))

## File holding error postcodes
pcodeserrorfile = "postcodes_errors.csv"
errorfile1 = os.path.abspath("{}/{}".format(pcodespath, pcodeserrorfile))

logpath = r'./logs/'
logname = r'{}_log.txt'.format(today)


## Change directory if running this in a batch script
os.chdir(currentpath)


## This URL takes us to the initial location page (to confirm search criteria)
dbname = 'dbname.db'             ## Name of DB
pcodes = os.path.abspath(pcodesfile) #r'./master_postcode_lists./postcodes_list_master.csv'      ## Name of london postcodes .csv file

## Name of shelve
## This is used
basic_dbtemplate = 'basic_{}_{}'        ## String to hold cache of property information in format 'basic_SE17_20180102'
propinfo_dbtemplate = 'propinfo_{}_{}'  ## String to hold cache of basic info for property

## Property web information
url = ""
url_sale = "/property-for-sale"
url_search = url + url_sale + "/search.html"
url_find = url + url_sale + "/find.html"

result_per_page = 24        ## Number of results per page (this might 

## If logging to external file
if logging:
    ## Set logfile as object, open as file and set object as log
    logfile = ft.FileOperations(logpath, logname)
    logfile.openFile()
    logfile.setAsLog()

###########################################
## Delete old files cached during processing
###########################################
prefixes = "basic_ propinfo_".split()


## Check whether user wants to be asked
ask = True
#pdb.set_trace()
if len(sys.argv) > 1:
    if sys.argv[1] == 'Y':
        ask = False

    
#pdb.set_trace()
for p in prefixes:
    ft.clearOldFilesWithPrefix(prefix=p, ask=ask)




##############################################
## Postcode information
##############################################

## seed = today's date
#seed = int(datetime.today().strftime('%Y%m%d'))
seed = datetime.now().strftime('%Y%m%d%H%M%S')
postcodes = ft.SelectRandomPostcodes2(seed, pcodes, dbname, 80)      ## -1 for al
postcodes = postcodes[::-1]
#postcodes = "E3".split()

## Load postcode information
print("Loaded {} postcodes from file with name {}".format(len(postcodes), pcodes))
print("Loaded postcodes are: {}".format(postcodes))
#

## Connect to SQL database
con = sqlite3.connect(dbname)
c = con.cursor()



"""
WRITE A FUNCTION WHICH MAKES A CONNECTION TO THE DB
IF IT CAN.  IF NOT, IT KEEPS TRYING
THE DB IS CLOSED WHEN A CHANGE IS MADE
ANDREW CRAIK, 2018-03-19
"""

## List to hold properties with errors
errors = []

## SQL statements to be updated
sql_statements = []

## Update timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')


## Init postcode count (can't remember where this is used)
pCodeCount = 0

if not isinstance(postcodes, list):
    print("Sorry cannot continue as object postcodes is not of type 'list' is is of type {}".format(type(postcodes)))
## For each postcode in postcode list
for p in postcodes:

    pCodeCount += 1

    #
    
    try:        
    
        ##############################################
        ## Initialise some lists/dictionaries etc.
        ##############################################
        
        ## Init property URLs list (basic information from search results)
        props_basic = []

        ## Re-initalise actual property information lists
        prop_info = []

        ##  Update name of basic DB for this tempalte today
        basic_db = basic_dbtemplate.format(p, today)

        ## Update the name of proeprty information for this template today
        propinfo_db = propinfo_dbtemplate.format(p, today)
        
        ## If basic_db already exists, then load it and loop through them
        if os.path.isfile('{}'.format(basic_db)):
            
            print("\nBasic information for postcode '{}' already cached with name '{}'.  Loading this.".format(p, basic_db))
            ## Load information from shelf
            with open(basic_db, 'rb') as f: props_basic = pickle.load(f)
            
                  
        ## Else, check whether the property information has been saved
        else:
            print("Getting basic information on properties from search results for properties in {}: {} of {}".format(p, pCodeCount, len(postcodes)))

            ###############################################################
            ## Get location information - outcode from postcode
            ###############################################################

            ## With postcode get property sites outcode
            locationIdentifier = ft.getLocationID(url_search, url_find, p)
            #

            ## Set up query, index used to loop through search results
            payload = {"locationIdentifier":locationIdentifier
                       , "index":0
                       }

            ##############################################################
            ## Get all pages of search results for given postcode
            ##############################################################
            
            ## With location - get property URLs
            props_basic.extend(ft.getAllPropertyURLs(p
                                                      , url_find
                                                      , url
                                                      , result_per_page
                                                      , payload))

            ## Cache search results/basic information too pickled file
            with open(basic_db, 'wb') as f: pickle.dump(props_basic, f)
            
        
        ##################################################################
        ## Now get the information for each property
        ##################################################################

        ## Check whether the property information has already been
        if os.path.isfile('{}'.format(propinfo_db)):

            ## Give user information on this
            print("Property information for the '{}' postcode has already been saved with name {}. Re-loading this.".format(p, propinfo_db))
            
            ## Load information from pickled object
            with open(propinfo_db, 'rb') as f: prop_info = pickle.load(f)
            

        ## Else, no cached information on this postcode (p)
        else:
            
            print("No cached information on this postcode '{}'".format(p))    
            print("Looping through each of the {} basic properties for postcode {}".format(len(props_basic), p))

            for i in range(len(props_basic)):
                
                ## Update timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    
                #print("URL number {1} Property summary: {0} ".format(props_basic[i]['propertyDescription'], i))
                next_info = ft.getPropertyInfo(c, props_basic[i], i, len(props_basic))

                ## If dictionary found, add to list
                if next_info != {}:
                    prop_info.append(next_info)
                    
            ## Cache search results/basic information too pickled file
            with open(propinfo_db, 'wb') as f: pickle.dump(prop_info, f)
            
           
 
        ###################################################################
        ## Now update table
        ###################################################################
        #print("Looping through each of the {} property inforations for postcode {}".format(len(prop_info), p))
        iPropCount = 0
        for prop in prop_info:
            iPropCount += 1
                                
            try:
                
                result = ft.SQLtoUpdateTable(c, sql_statements, timestamp, prop)
            
                if result == []:
                    sql_statements = []
                
            except:
                print("ERROR: Error in python code trying to update property with ID {}".format(prop_info[prop]['propertyId']))
                next
                
      

        
    ## If something goes wrong while running for this postcode           
    except RuntimeError as e:
        print("Runtime error getting property information at iteration {}: ".format(i, e))
        ft.TidyUp('runtime', e, con, c, sql_statements, timestamp, prop_info)
        
    except ValueError as e:
        print("Value error: {}".format(e))
        ft.TidyUp('valueerror', e, con, c, sql_statements, timestamp, prop_info)
        
    except TypeError as e:
        print("Type error: {}".format(e))
        ft.TidyUp('typeerror', e, con, c, sql_statements, timestamp, prop_info)
        
    except KeyboardInterrupt as e:
        print("\nKeyboard interrupt")
        check = input("Do you want to stop the entire program? Y/N: ")
        ft.TidyUp('User stop', e, con, c, sql_statements, timestamp, prop_info)
        ### If user wants to continue let the program continue
        if check.upper() == 'Y':
            pass
        else:
            break

    ## No data for postcode
    except IndexError as e:
        print("Index error in search: {}".format(e))
        with open(errorfile1, 'a') as f:
            f.write("{},{},{}\n".format(p, datetime.now().strftime('%Y%m%d%H%M%S'),e))
        continue
        
    ## All else, fails, update DB with progress
    except:
        print("\nOther error") 
        ft.TidyUp('other', None, con, c, sql_statements, timestamp, prop_info)
        raise

#pdb.set_trace()
## If sql statements not been updated (after running all postcodes)
if sql_statements != []:
    print("\nFinished processing {} postcodes".format(len(postcodes)))
    ft.TidyUp(None, None, con, c, sql_statements, timestamp, prop_info)

    
## Close DB
con.close()
try:
    logfile.closeFile()
except:
    print("Log file wasn't open - nothing to close")

## Raise last error
#raise
