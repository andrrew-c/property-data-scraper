"""
Name: Flats Tools - Property Data
Purpose: This script contains a number of functions which are used to control
        the web scraper.


        
Description: 
Author: Andrew Craik 
Date: February 2018

"""
import requests
from bs4 import BeautifulSoup as bs
import json
import sqlite3
from datetime import datetime
import time
import collections
import os
import sys

## Random
import random 

import csv


# Debug
import pdb
#import testing_db_update as tdu


def getLocationID(url_search, url_find, postcode):

    """
    Returns the outcode when given a postcode and some soup
    The outcode is a code used by the property website from postcode->outcode
    Returns string in format "OUTCODE^xxxxx"

    Inputs:
        url_search: string holding the first part of the property website search
        url_find: string containing the 'find properties' url - used to check whether searching a REGION or an OUTCODE
        postcode: The postcode of interest
    """

    ## Create full URL
    url_location = url_search + "?searchLocation={}&locationIdentifier=&useLocationIdentifier=false&buy=For+sale".format(postcode)

    ## Make request, then soup
    res_location = requests.get(url_location)
    #with open('res.txt', 'wb') as f: f.write(res_location.content)
    #print("******** Writing out to a text file")
    soup = bs(res_location.content, 'html.parser')

    ## Find the outcode dictionary
    location_id = soup.find_all('input', id="locationIdentifier") 
    
    ## Outcode
    rm_outcode = location_id[0]['value']

    ##################################################
    ## Some places need a 'REGION' some 'OUTCODE'
    ##################################################
    
    ## Need to test a request
    sCode = "OUTCODE REGION".split()
    locationIdentifier = [i + rm_outcode[rm_outcode.find('^'):len(rm_outcode)] for i in sCode]

    ## Try both
    for i in range(len(sCode)):
        ## Make request
        res = requests.get(url_find, params={'locationIdentifier':locationIdentifier[i], 'index':0})

        ## Request status
        res_stat = str(res.raise_for_status)
        status = res_stat[res_stat.find('[')+1:res_stat.find(']')]

        ## Check that the status works
        if not status == '404':
            print("Using location code type '{}'".format(sCode[i]))
            return locationIdentifier[i]

    ## Else, need to raise an error
    print("ERROR (getLocationID): no match found in '{}'".format(sCode))

    


def getPropertyURLs(soup):
    
    """
        DISCONTINUED
        Returns a list of the property URLs for a given postcode (or area)
    """
    ## Properties (?)
    props1 = soup.find_all('a', class_='propertyCard-link')
    props2 = [i for i in props1 if i.find('address')]

    ## Extract properties URLs
    prop_urls = [i['href'] for i in props2 if i['href']!='']
    #
    return prop_urls


def getPropertyBasicInfo(url, soup):
    
    """
        Returns a list of dictionaries holding basic information on each property
        in the search results
    """
    
    ##Now pull out some basic information on all properites
    scripts = soup.find_all('script')
    sc = [s.text for s in scripts if str(s).find('jsonModel')!=-1 and str(s).find('properties')!=-1]
    
    ## Extract JSON text string from scripts
    try:
        js_txt = [s[s.find('{'):] for s in sc][0]
        js_dict = json.loads(js_txt)

            
        
        ## All the other information comes from the individual
        ## We take, ID, summary, display address
        ##, property description, property URL, property images

        ## Init property info list
        props = []
        

        ## Loop through each property in JSON data
        for p in js_dict['properties']:

            ## Added or reduced - separate
            addOrReduceDate = ''
            ## If 'on' not found in string (i.e. 'on dd/mm/yyyy')
            
            if p['addedOrReduced'].find('on') == -1:
                addOrReduce = p['addedOrReduced']
                
                

            ## Else, date string contained
            else:
                addOrReduce = p['addedOrReduced'][:p['addedOrReduced'].find('on')-1]
                addOrReduceDate = p['addedOrReduced'][p['addedOrReduced'].find('on')+3:]

            ###
            ## Extract information on property
            props.append({'propertyId':p['id']
                                 , 'propURL':url+p['propertyUrl']
                                 , 'displayAddress':p['displayAddress']
                                 , 'propertyDescription':p['propertyTypeFullDescription']
                                 , 'listingUpdateReason':p['listingUpdate']['listingUpdateReason']
                                 , 'listingUpdateDate':p['listingUpdate']['listingUpdateDate']
                                 , 'addedOrReduced':addOrReduce
                                 , 'addedOrReducedDate':addOrReduceDate
                             })
            
            
     
        ## Returns the list
        return props
    except:
        return []

def getAllPropertyURLs(postcode, url_find, url, result_per_page, payload):

    """
        
        Given a URL this function will return a list of the unique URLs
        related to properties.

        For example, URL will relate to the search terms for a single postcode area,
        this function will iterate through each of the pages to get all of the properties URLs
        and return those as a list, removing duplicates.
        
    """

    
    ## Init list to hold all URLs
    all_props = []

    ## Init loop stop to False
    blank_results = False

    ## Init idx to zero
    idx = 0

    ## Sleep time in second
    sleeptime = 2
    
    ## While resulsts are not blank
    while not blank_results:

        ## Sleep time between iterations
        print("getAllPropertyURLs: Sleeping for {} seconds.".format(sleeptime))
        

        ## Update query payload
        payload.update({'index':idx*result_per_page})

        ## Make request
        print("\nMaking request for postcode {}, page = {}".format(postcode, idx+1))
        res = requests.get(url_find, params=payload)
        time.sleep(sleeptime)

        #with open('res.txt', 'wb') as f: f.write(res.content)
        #print("******** Writing out to a text file")
        
        ## Soup
        print("Making soup for postcode '{}'".format(postcode))
        soup = bs(res.content, 'html.parser')

        ##  Get properties results for a single iteration
        print("Getting properties URLs")
        
        new_list = getPropertyBasicInfo(url, soup)
        #
        print("\nThere are {:,} properties in the current list".format(len(all_props)))

        ## If there are properties in the soup
        if len(new_list) > 0:
            
            ## Add URLs to list
            all_props.extend(new_list)
            print("Current list of properties now = {:,}".format(len(all_props)))
            #

            ## Iterate index loop
            idx += 1
        else:
    
            ## Else, we have run out of pages, return list
            final_result = []
            #
            final_result.append(all_props[0])
            for p in range(1, len(all_props)):
                
                if all_props[p]['propertyId'] not in [i['propertyId'] for i in final_result]:
                    final_result.append(all_props[p])
            #               
            return final_result


def sleepProgram(bigProcess=False):

    """
        For big processes we should have 15 seconds inclued
        For others, no need (e.g. iterating through serach results)
    """
    
    ## Make sure process doesn't throttle website
    rand = random.uniform(0,1)
    if bigProcess and rand>0.8:
        sleep = 15
        print("Big process, sleeping for {} seconds.".format(sleep))
        time.sleep(sleep)

    
    else:
        if rand>0.6:
            sleep = 5
            print("getPropertyInfo: Sleeping for {} seconds".format(sleep))
            time.sleep(sleep)
        else:
        
            time.sleep(1)


def getPropertyInfo_TidyVars(basic_dict):
    pass
    #return dictionary

    ## Price
    tidy = {'price':{None:-9}}
    
    for k in basic_dict.keys():

        ## If this key should be tidied
        if k in tidy.keys():
            
            if basic_dict[k] in tidy[k].keys():
                #
                basic_dict.update({k:tidy[k][basic_dict[k]]})
                
    return basic_dict
                              
def getPropertyInfo(c, basic_dict, index, ofSize):
    """
        Purpose: Get the property information for a single property (given the basic information scraped from the search results)
        Given a base URL (for the website) and a 'properties specific' URL, return a dictionary of attributes for the properties.

        c:          sqlite3 cursor for a connection
        basic_dict: dictionary holding the basic information for a single property gained from the search results
        index:      integer holding the index of the current property within the postcode
        ofSize:     integer holding the total size of properties in the postcode
    """

    #
    
    
    ## Separate URL for this properties
    #
    full_url = basic_dict['propURL']
    

    ## If listing update date contains a string - recode it
    #
    if basic_dict['listingUpdateDate'] != None:
        basic_dict.update({'listingUpdateDate2':basic_dict['listingUpdateDate']})
        basic_dict.update({'listingUpdateDate':None})
        #
    ## Else, give it something
    else:
        basic_dict.update({'listingUpdateDate2':None})

    ## Copy original for final 
    final_result = dict.copy(basic_dict)
    
    ## If property has chanegs
    if propertyShouldBeUpdated(c, basic_dict):
        
        ## Updating property information to log
        print("Property {} of {} ({:.1%}) with ID {} being updated/loaded".format(index+1, ofSize, (index+1)/ofSize, basic_dict['propertyId']))
        prop_res = requests.get(full_url)
        
        sleepProgram(True)

        ## Trying to make soup for properties - strugglingencoding = r.encoding if 'charset' in r.headers.get('content-type', '').lower() else None
        soup_prop = bs(prop_res.content, 'html.parser')
        #print("Length of soup = {}".format(len(soup_prop)))
        #with open('property_info.txt', 'wb') as f: f.write(prop_res.content)

        ########################################################
        ## Now with properties soup - let's get the information
        ########################################################

        prop_info = [i for i in soup_prop.find_all('script') if i.text.find('(function(k,v)')!=-1]
        #
        #print("Length of properties info for a single properties = {}".format(len(prop_info)))

        if len(prop_info)>0:
            
            ## Need to turn the key information into a dictionary
            js_text = prop_info[0].text[prop_info[0].text.find('property'):]
            
            try:
                   
                prop_dict = json.loads(js_text[js_text.find('{'):js_text.find(')')])
                #
                ## Key features (held separately) and full description
                other_sections = soup_prop.find_all('div', class_='sect')

                kf_string = 'Key features'
                fd_string = 'Full description'

                ## Key features (if there) and full description, separate             
                kf_l = [i.text for i in other_sections if i.text.find(kf_string)!=-1]
                fd_l = [i.text[i.text.find(fd_string)+len(fd_string):] for i in other_sections if i.text.find(fd_string)!=-1]
                
                ## Extract text from first element of list, if there is any
                kf_full = extractFirstIfListHasIt(kf_l)
                #
                kf = None
                if kf_full != None:
                    if len(kf_full) > 0:
                        kf = getKeyFeatures(kf_full, kf_string)
                
                
                tenure, fd = extractTenureAndDescription(fd_l)
                
                ## Images
                #print("Getting image URLs")
                images = soup_prop.find_all('meta', itemprop='contentUrl')

                ## Now isolate the image content
                imgs = [i['content'] for i in images]
                #

                #
                #print("\nURL = {}".format(prop_url)) # and number of floor plans = {}".format(prop_url, prop_dict['floorplanCount']))    

                ## Floor plan
                ## Reset floor plan list
                fp_imgs = []
                if prop_dict['floorplanCount'] >= 1:

                    ## List all image links
                    all_images = soup_prop.find_all('img')

                    ## Isolate the 'src' tag from any tags which contain 'FLOOR' - unique list
                    fp_imgs = list(set([i['src'] for i in all_images if i.prettify().upper().find('FLOOR')!=-1]))
                    
                    #    
                
                ## Update dictionary: Key features, full description, images
                prop_dict.update({'tenure':tenure
                                     , 'fullDescription':fd
                                     , 'key_features': kf
                                     , 'imageURLs':imgs
                                     , 'floorplan':fp_imgs})
                
                ## Bring together
                final_result.update(flatten(prop_dict))

                ## Tidy up vars
                #
                final_result = getPropertyInfo_TidyVars(final_result)
                        
                ## Finally, return dictionary
                return final_result

            except:
                strUpdate = "ERROR: Please check dictionary for property with URL '{}'\n".format(full_url)
                strStars = "*"*len(strUpdate)
                print("{0}\n{1}\n{0}".format(strStars, strUpdate))

                ## Add to errors list
                #errors.append(basic_dict)
                #
                return {}
    ## Else, property should not be updated in DB
    else:
        #print("No need to request property information")
        #
        lastSeen = {'propertyId':basic_dict['propertyId'], 'lastFoundInSearch':datetime.today().strftime('%Y%m%d')}
        
        return lastSeen

         

def getKeyFeatures(string, ignore_string):
    """
    Return list of key features
    """
    
    ## Split string into list
    str_list = [i.replace(ignore_string, '').replace("'",'').replace('"','') for i in string.split('\n')]
    str_list2 = [i for i in str_list if i != '']

    ## Return values
    #
    return str_list2
    

def extractFirstIfListHasIt(listname, info='text'):
    """
        Returns the first element's text or other information
    """

    if len(listname)>0:
        ## If user hasn't specified type of info, then get text
        if info == 'text':
            return listname[0]
        ## Otherwise, get the specified information
        else:
            return listname[0][info]
    else:
        return None

def extractTenureAndDescription(listname):
    """
        Returns the first element's text or other information
    """

    ctr = "\0".split()
    charsToRemove = str.maketrans(dict.fromkeys(ctr))
    #
    ## String used to find tenure
    tenure_string = 'Tenure:'

    ## If list has some information in it, look for tenure and/or full description
    if len(listname)>0:

        ## Simpler to type
        fullstring = listname[0]
        
        ## TENURE ##
        
        ## If tenure string in text
        if fullstring.find(tenure_string)!=-1:
            
            ## Remove the tenure string from text
            t1 = fullstring[fullstring.find(tenure_string)+len(tenure_string):]
            

            ## Now keep tenure type to output
            tenure_type = t1[:t1.find('\n')].replace(' ', '').upper()
            #
            ## Now get the rest of the string

            
            fullDescription = t1[t1.find('\n')+1:].strip().replace('"', "'").translate(charsToRemove)
            
            return tenure_type, fullDescription
        ## Else, tenure string not found
        else:
            ## Else, no tenure, but there is a 'full description'
            fullDescription = fullstring[fullstring.find('\n')+1:].strip().replace('"', "'")
            return None, fullDescription
    else:
        return None, None

    
def getPropertyVariables(dictionary):

    ######################################
    ## properties information
    ######################################
    
    propertyId              = dictionary['propertyId']
    propURL                 = dictionary['propURL']
    added                   = dictionary['added']
    listingUpdateReason     = dictionary['listingUpdateReason']
    listingUpdateDate       = dictionary['listingUpdateDate']
    listingUpdateDate2      = dictionary['listingUpdateDate2']
    addedOrReduced          = dictionary['addedOrReduced']
    addedOrReducedDate = dictionary['addedOrReducedDate']
    propertyDescription = dictionary['propertyDescription']
    fullDescription     = dictionary['fullDescription']
    propertyType    = dictionary['propertyType']
    propertySubType = dictionary['propertySubType']
    price           = float(dictionary['price'])
    beds            = int(dictionary['beds'])
    tenure          = dictionary['tenure']
    soldSTC         = 1 if dictionary['soldSTC'] == True else 0
    retirement      = dictionary['retirement']
    preOwned        = dictionary['preOwned']
    ownership       = dictionary['ownership']
    auctionOnly     = 1 if dictionary['auctionOnly'] == True else 0
    letAgreed       = 1 if dictionary['letAgreed'] == True else 0
    lettingType     = dictionary['lettingType']
    furnishedType   = dictionary['furnishedType']
    minSizeFt       = dictionary['minSizeFt']
    maxSizeFt       = dictionary['maxSizeFt']
    minSizeAc       = dictionary['minSizeAc']
    maxSizeAc       = dictionary['maxSizeAc']
    businessForSale = 1 if dictionary['businessForSale'] == True else 0
    priceQualifier  = dictionary['priceQualifier']
    currency        = dictionary['currency']

    ## Return values
    return propertyId, propURL, added \
            , listingUpdateReason, listingUpdateDate, listingUpdateDate2, addedOrReduced, addedOrReducedDate \
            , propertyDescription, fullDescription, propertyType, propertySubType \
            , price, beds, tenure \
            , soldSTC, retirement, preOwned, ownership \
            , auctionOnly, letAgreed, lettingType, furnishedType \
            , minSizeFt, maxSizeFt, minSizeAc, maxSizeAc \
            , businessForSale, priceQualifier, currency

def dict_factory(cursor, row):
    """ Returns a dictionary from row ('header':value) """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def SQLGetMostRecentRecord(c, tablename, propertyId):

    """ With cursor and propertyID return dictoinary"""
    
    
    ## Check whether any variables differ
    sql_return = """SELECT *
                    FROM {}
                    WHERE propertyId = {}
                    GROUP BY propertyId
                    HAVING MAX(timestamp)
            """.format(tablename, propertyId)

    ## Get results from table for this properties
    result = runSQLCommands(c, [sql_return], getResults=True)[0]
    
    ## Result used to name columns too
    #
    ## Get dictionary of most recent update
    if result != None:
        return dict_factory(c, result)
    
    ## Else, return empty dictionary
    else:
        return {}

def SQLGetAllRecords(c, tablename, propertyId, valuename):

    ## Initialise result
    # records_values = []

    ## SQL to be executed - returns all values that match the latest timestamp
    sql_return = """SELECT A.{1}
                        FROM {2} AS A
                        INNER JOIN
                            (SELECT DISTINCT propertyID, timestamp
                                FROM {2}
                                WHERE propertyID = {3}
                                GROUP BY propertyId
                                HAVING COUNT(*) > 1 AND MAX(timestamp)
                            ) AS B
                        ON A.propertyID = B.propertyID
                            and A.timestamp = B.timestamp                    """.format("propertyId", valuename, tablename, propertyId)
    
    ## Get results from table for this properties
    result = runSQLCommands(c, [sql_return], getResults=True)
##    c.execute(sql_return)
##    result = c.fetchall()

    ## Put values into single list, or empty list []
    records_values = [i[0] for i in result] if result!=None else []

    ## Return list holding values (either list of property features, or image URLs
    return records_values
    


def propertyShouldBeUpdated(c, dictionary):

    """
        With SQL connection check whether table should be updated

        A property should be upadted if:
            - propertyid not in DB
            - if the listing update date in scraped data is after the last one
    """

    #

    ## Property ID we're interested in
    propid = dictionary['propertyId']

    ## SQL to pull out from DB
    sql = """SELECT propertyId
                , listingUpdateDate
                , addedOrReducedDate
                , added
                , listingUpdateDate2
                , timestamp
                    FROM property
                    WHERE propertyId = {}
                    GROUP BY propertyId
                    HAVING timestamp = MAX(timestamp)
                    """.format(propid)

    ## Excecute SQL
    c.execute(sql)
    result = c.fetchone()
    
    
    ## If property doesn't exist already in DB, then update it (function ends)
    if result == None:
        return True

    ## listingUpdateDate2: Else, check scraped dictionary update date matches the DB, return False
    elif dictionary['listingUpdateDate2'] == result[4]:
        return False

    # We need to check the data in the scraped data
    else:
        
        ## Scraped date from website
        newListDate = dictionary['listingUpdateDate2'] if dictionary['listingUpdateDate'] == None else dictionary['listingUpdateDate']
            
        ###########################################################
        ## Check if listing update date contains a string
        ## This is something that was added after the program
        ## was first written
        ###########################################################
        
        if newListDate == None:

            if result[5] != '':
                ## dbDate is integer (or 'None')
                dbDate = datetime.strptime(result[5][:8], '%Y%m%d')

                ## If scraped date is after
                if newListDate > dbDate:
                    return True
                ## Else, no need to update
                else:
                    return False
        
        ## If string contains a time element
        elif newListDate.find('T') != -1:

            ####################################
            ## Isolate date from from DB
            ####################################
            
            ## If the 'addedOrReducedDate' is null then the best I can do is check the 'added' date
            if result[2]=='':
                
                ## Added date
                if result[3] != 'None':       
                    dbDate = datetime.strptime(str(result[3]), '%Y%m%d')
                    
                ## Else, there are no dates in DB, use the timestamp
                else:
                    print("Called by '{}'.\nThere are no dates for propertyid '{}', using timestamp.\nWorth checking".format(__file__, result[0]))   
                    dbDate = datetime.strptime(result[5][:8], '%Y%m%d')
                    

            ## Else, take the date from 'addedOrReducedDate'
            else:
                dbDate = datetime.strptime(result[2], '%d/%m/%Y')
                    
            ## Now, extract the date and compare with DB value (addedOrReducedDate)
            day = newListDate[8:10]
            month = newListDate[5:7]
            year =  newListDate[:4]

            ## Now combine into date string (which we'll use to write to DB)
            datestring = '/'.join([day, month, year])
            
            ## Create python date object to compare with addedOrReducedDate
            newListDate = datetime.strptime(datestring, '%d/%m/%Y')

            ## If scraped date is after the DB
            if newListDate > dbDate:
                return True
            ## Else, no need to update
            else:
                return False

        
        ###
        ## Else, the string for listingUpdateDate is in the numeric format, compare this
        else:
                
            ## dbDate is integer (or 'None')
            dbDate = result[1]

            ## If scraped date is after
            if newListDate > dbDate:
                return True
            ## Else, no need to update
            else:
                return False
        

                

def SQLtoUpdateTable(c, sql_statements, timestamp, dictionary, forceRun=False):

    """ If SQL statements are over 1000 or forced to run, execute commands
        Return empty list if this happens"""

    ## Just check if force run has been provided - execute updates
    if forceRun and sql_statements != None:
              
        ## Run the commands
        runSQLCommands(c, sql_statements, 'to update DB')

        ## Reset list
        return []
    
    ## If, for whatever reason, the dictionary is null, return an empty list
    if dictionary == None:
        return []
    
    ## Else, do something
    else:
    
        ## If SQL commands >= 1000
        if forceRun or len(sql_statements) > 2000:
            print("Forcerun = {} and len(sql_statements)={}".format(forceRun, len(sql_statements)))
                    
            ## Run the commands
            runSQLCommands(c, sql_statements, 'to update DB')

            ## Reset list
            return []
        
        ## Else, DB is not to be run, return updated SQL statements
        else:
        
            ## Init sql commands (individual ones)
            sql_command_property = ''
            sql_command_location = ''
            sql_command_features = []
            sql_command_images   = []

            ## If property should be added to DB
            ## (i.e. listingUpdateDate is > than DB or ID not in DB)
            #

            ## I chose to check for propURL as it will not be in the dictionary
            ## if the property hasn't been updated
            if dictionary.get('propURL') != None:
                
                sql_command_property = SQLProperty(dictionary, timestamp)
                sql_command_location = SQLLocation(dictionary, timestamp)
                sql_command_features = SQLFeatures(dictionary, timestamp)
                sql_command_images = SQLImages(dictionary, timestamp)

                sql_commands = [sql_command_property, sql_command_location] \
                                    + sql_command_features  + sql_command_images
                
                
            ## Else, no need to update this property in DB
            else:
                #print("Only updating 'last seen' date for ID = {}".format(dictionary['propertyId']))
                sql_commands = [SQLPropertyUpdateLastSeen(dictionary, timestamp)]

            ## Sql statements should be extended with list
            sql_statements.extend(sql_commands)       
            
            
            ## Sql commands
            return sql_statements

def runSQLCommands(c, sql_commands, string, getResults=False):

    """
        This will attempt to execute the SQL commands to the cursor (c)
        if getResults is True then the function will return the values
    """
    
    ## Init boolean to keep retrying, keep count of number
    committment_needed = True
    iAttempts = 0

    ## Integer holding number of updates (to print to output)
    iCnt = 0

    ## Keep trying until committment = False (i.e. successfully run)
    while committment_needed:

        ## Iterate number of attempts
        iAttempts += 1
        print("Attempt {} to {}\n".format(iAttempts, string))
        
        ## Loop though each command, execute and commit
        for s in sql_commands:

            ## Number of SQL statements executed
            iCnt += 1
            #print(iCnt)
            if iCnt%100 == 0:
                print("runSQLCommands: Updating DB iteration {}".format(iCnt))
                
            
            try:
                
                c.execute(s)
                
            
            except sqlite3.OperationalError as e:
                #print(s)
                print("ERROR: '{}'\nWill wait, then try again - 15 seconds".format(e))
                time.sleep(15)
                break
            except ValueError as e:
                print(e)
                next
            except:
                raise 

        ## We've run through all the SQL commands - turn of the infinite loop
        committment_needed = False

        
        ## Now, we seem to have executed the SQL commands
        ##- so let's try and commit the changes
        ## Seems like all is working - let's get some committment going in our lives!
        print("Committing changes to DB")
        c.connection.commit()

    ## Once, the query has been run, If user wants, results, return them
    if getResults:
        res = c.fetchall()
        return res


def update_table_old(con, c, timestamp, dictionary):

    ####################################################
    ### THINK THIS IS NO LONGER USED - 2018-03-04
    ####################################################
    
    ## Init sql commands (individual ones)
    sql_command_property = ''
    sql_command_location = ''
    sql_command_features = []
    sql_command_images   = []

    ## If property should be added to DB
    ## (i.e. listingUpdateDate is > than DB or ID not in DB)
    if dictionary.get('listingUpdateDate') != None:
        sql_command_property = SQLProperty(dictionary, timestamp)
        sql_command_location = SQLLocation(dictionary, timestamp)
        sql_command_features = SQLFeatures(dictionary, timestamp)
        sql_command_images = SQLImages(dictionary, timestamp)

        sql_commands = [sql_command_property, sql_command_location] \
                            + sql_command_features  + sql_command_images

        for s in sql_commands:
            #
            c.execute(s)

        ## Init boolean and counter
        committment_needed = False
        iAttempts = 0

        ## Keep trying
        while committment_needed:
            iAttempts += 1
            print("Attempt {} to commit changes to DB".format(iAttempts))
            try:
                con.commit()
                committment_needed = False
            except sqlite3.OperationalError:
                print("Database locked.  Will wait, then try again - 15 seconds")
                time.sleep(15)

    ## Else, no need to update this property in DB
    else:
        #print("Only updating 'last seen' date for ID = {}".format(dictionary['propertyId']))
        sql_command_property = SQLPropertyUpdateLastSeen(dictionary, timestamp)
        c.execute(sql_command_property)
    con.commit()
        
def update_lastFoundInSearch(con, c, dictionary):
    
    propid = dictionary['propertyId']
        
    sql = """ UPDATE TABLE {}
                SET lastFoundInSearch = "{}"
                WHERE propertyId = {}
                GROUP BY propertyId
                HAVING timestamp = MAX(timestamp)
                
        """.format('property', today, dictionary['propertyId'])
    runSQLCommands(c, [sql])
##    c.execute(sql)
##    con.commit()
    #
    


def flatten(d, parent_key='', sep='_'):

    """ Flatten out nested dictionary"""
    
    items = []
    for k, v in d.items():

        #new_key = parent_key + sep + k if parent_key else k
        new_key = k
        if isinstance(v, collections.MutableMapping):
            
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
 
   

def SQLPropertyUpdateLastSeen(dictionary, timestamp):

    tablename = 'property'
    var = 'lastFoundInSearch'
    today = timestamp[:8]
    
    sql = """ UPDATE {0}
                SET {1} = '{2}'
                WHERE propertyID = {3}
	AND timestamp = (SELECT timestamp
                            FROM {0}
                            WHERE propertyID = {3}
                            GROUP BY propertyID
                            HAVING MAX(timestamp) = timestamp)
        """.format(tablename, var, today, dictionary['propertyId'])
    #print(sql)
    #
    return sql
    
def SQLProperty(dictionary, timestamp, changes='None'):
    """
    Returns the sql command to insert a properties record
    """
    pass

    ######################################
    ## properties information
    ######################################

    propertyId, propURL, added \
        , listingUpdateReason, listingUpdateDate, listingUpdateDate2, addedOrReduced, addedOrReducedDate \
        , propertyDescription, fullDescription, propertyType, propertySubType \
        , price, beds, tenure \
        , soldSTC, retirement, preOwned, ownership \
        , auctionOnly, letAgreed, lettingType, furnishedType \
        , minSizeFt, maxSizeFt, minSizeAc, maxSizeAc \
        , businessForSale, priceQualifier, currency = getPropertyVariables(dictionary)
    #

    ## properties information
    
    sql = """INSERT INTO property
                        (propertyId, timestamp, propURL, added
                            , listingUpdateReason, listingUpdateDate, listingUpdateDate2, addedOrReduced, addedOrReducedDate
                            , propertyDescription, fullDescription, propertyType, propertySubType
                            , price, beds, tenure
                            , soldSTC, retirement, preOwned, ownership
                            , auctionOnly, letAgreed, lettingType, furnishedType
                            , minSizeFt, maxSizeFt, minSizeAc, maxSizeAc
                            , businessForSale, priceQualifier, currency, changes)
                        VALUES ({}, "{}", "{}", "{}"
                                , "{}", "{}", "{}", "{}", "{}"
                                , "{}", "{}", "{}", "{}"
                                , {}, {}, "{}"
                                , {}, "{}", "{}", "{}"
                                , {}, {}, "{}", "{}"
                                , "{}", "{}", "{}", "{}"
                                , {}, "{}", "{}", "{}" )
                                """.format(propertyId, timestamp, propURL, added
                                            , listingUpdateReason, listingUpdateDate, listingUpdateDate2, addedOrReduced, addedOrReducedDate
                                            , propertyDescription, fullDescription, propertyType, propertySubType
                                            , price, beds, tenure
                                            , soldSTC, retirement, preOwned, ownership
                                            , auctionOnly, letAgreed, lettingType, furnishedType
                                            , minSizeFt, maxSizeFt, minSizeAc, maxSizeAc
                                            , businessForSale, priceQualifier, currency, changes)
    #
    return sql

def SQLLocation(dictionary, timestamp):

    ########################################
    ## Location
    ########################################

#   ## Pull out the values
    #loc_d       = dictionary['location']
    propertyId      = int(dictionary['propertyId'])
    displayAddress  = dictionary['displayAddress']
    postcode        = dictionary['postcode']
    country         = dictionary['country']
    lat             = dictionary['latitude']
    long            = dictionary['longitude']

    ## Location information
    sql = """INSERT INTO location (propertyId, timestamp, displayAddress, postcode, country, latitude, longitude) 
                    VALUES ({}, "{}", "{}", "{}", "{}", "{}", "{}")
            """.format(propertyId, timestamp, displayAddress, postcode, country, lat, long)
    return sql

def SQLImages(dictionary, timestamp):
    """
    Returns the a list of sql commands to insert a new image record
    """
    pass

    sql = []

    ##########################################
    ## properties images (including floorplan)
    ##########################################

    propertyId      = dictionary['propertyId']
    imageURLs       = dictionary['imageURLs']
    floorplanURLs   = dictionary['floorplan']

    ## properties images
    for i in range(len(imageURLs)):
        #
        sql.append("""INSERT INTO images (propertyId, timestamp, num, imgType, imgURL)
                        VALUES ({}, "{}", {}, "{}", "{}")
                    """.format(propertyId, timestamp, i+1, 'Image', imageURLs[i]))

    ## properties floor plans
    for f in range(len(floorplanURLs)):
        #
        sql.append("""INSERT INTO images (propertyId, timestamp, num, imgType, imgURL)
                        VALUES ({}, "{}", {}, "{}", "{}")
                    """.format(propertyId, timestamp, f+1, 'Floorplan', floorplanURLs[f]))
    return sql

def SQLFeatures(dictionary, timestamp):
    """
    Returns a list of sql commmands to insert a new feature
    """
    pass

    #########################################
    ## Key features
    #########################################
    
    kfs = dictionary['key_features']
    propertyId = dictionary['propertyId']
    
    sql = []
    
    if kfs != None:
        for kf in range(len(kfs)):
            sql.append("""INSERT INTO features (propertyId, timestamp, num, feature)
                            VALUES ({}, "{}", {}, "{}")
                            """.format(propertyId, timestamp, kf+1, kfs[kf]))
    return sql


  
def SQLpropExists(c, propertyId):

    """ Return True if ID exist in table, False, otherwise"""

    ## Select ID from properties table
    sql = """SELECT propertyId FROM properties where propertyId = {0}""".format(propertyId)

    ## Execute and fetch results
    result = runSQLCommands(c, [sql], getResults=True)[0]
##    c.execute(sql)
##    result = c.fetchone()
    
    ## If there is at least observation, return True
    if result != None:
        #sql_property(propertyId)
        return True
    ## Else, properties not found, return False
    else:
        return False
    
def create_tables(dbname):
    """
    This function creates a database (if not already existing)
        and then creates the required tables (if they don't exist
    """
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    ## Location information
    c.execute("""CREATE TABLE IF NOT EXISTS location
                    (propertyId INTEGER
                        , timestamp STRING
                        , displayAddress STRING
                        , postcode STRING
                        , country STRING
                        , latitude STRING
                        , longitude STRING
                        , changes STRING)""")

    
    ## properties information
    c.execute("""CREATE TABLE IF NOT EXISTS property
                    (propertyId LONG
                        , timestamp STRING
                        , propURL STRING
                        , added STRING
                        , listingUpdateReason STRING
                        , listingUpdateDate STRING
                        , addedOrReduced STRING
                        , addedOrReducedDate STRING
                        , propertyDescription STRING
                        , fullDescription STRING
                        , propertyType  STRING
                        , propertySubType STRING
                        , price DOUBLE
                        , beds INTEGER
                        , tenure STRING
                        , soldSTC BOOLEAN
                        , retirement STRING
                        , preOwned STRING
                        , ownership STRING
                        , auctionOnly INTEGER
                        , letAgreed BOOLEAN
                        , lettingType STRING
                        , furnishedType STRING
                        , minSizeFt STRING
                        , maxSizeFt STRING
                        , minSizeAc STRING
                        , maxSizeAc STRING
                        , businessForSale BOOLEAN
                        , priceQualifier STRING
                        , currency STRING
                        , changes STRING)""")
    ## Key features
    c.execute("""CREATE TABLE IF NOT EXISTS features
                (propertyId LONG
                    , timestamp STRING
                    , num INTEGER
                    , feature STRING)""")
    ## Images
    c.execute("""CREATE TABLE IF NOT EXISTS images
                    (propertyId LONG
                        , timestamp STRING
                        , num INTEGER
                        , imgType STRING
                        , imgURL STRING)""")

    ## Postcode/Outcode lookup
    sql = """CREATE TABLE IF NOT EXISTS postcode
                    (postcode STRING
                    , timestamp STRING
                    , outcode STRING)"""
    c.execute(sql)
    
    ## Commit changes
    conn.commit()

class FileOperations:
    
    def __init__(self, path, filename):
            
        self.filename = filename
        self.path = path
        self.fullpath = path + filename
    

        """Initialise file operation to write out log"""

        ## If log path doesn't exist
        if not os.path.isdir(path):
            
            ## output note to user and create directory
            print("Path {} doesn't exist\nCreating it in {}".format(path, currentpath))
            os.mkdir(path)
            

    ## Open files
    def openFile(self):
        self.file = open(self.fullpath, 'wt')
        

    ## Close files when done
    def closeFile(self):
        self.file.close()

    def setAsLog(self):
        
        print("Setting standard python output to '{}'".format(self.fullpath))
        sys.stdout = self.file



def TidyUp(exittype, e, con, c, sql_statements, timestamp, prop_info):

    
    if len(prop_info)>0:
        print("\nSome information is available in prop_info with {} properties.\nThis will be output to connection".format(len(prop_info)))

        ## For each property in property information
        for i in range(len(prop_info)):

            ## Try and get the SQL code to update the information retrieved
            try:
                
                result = SQLtoUpdateTable(c, sql_statements, timestamp, prop_info[i])
            
                if result == []:
                    sql_statements = []
                
            except:
                print("ERROR: Error in python code trying to update property with ID {}".format(prop_info[i]['propertyId']))
                raise
                next
                
        ## Run the commands
        print("Called by TidyUp.  Commit changes to DB")
        runSQLCommands(c, sql_statements, 'to commit changes to DB')
            
    else:
        print("No information was written to connection")
    print("\nClosing connection to DB and log")
    con.commit()
    con.close()
    
    try:
        logfile.closeFile()
    except:
        print("Log file wasn't open - nothing to close")


def TidyUpAll(prop_info, dbname):
    
    """ Tidy up all information as a last chance saloon (bit hard-coded)
    """
    #
    con = sqlite3.connect(dbname)
    c = con.cursor()
    sql_statements = []     ## Init
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if len(prop_info)>0:
        print("\nSome information is available in prop_info with {} properties.\nThis will be output to connection".format(len(prop_info)))

        ## For each property in property information
        for i in range(len(prop_info)):
            print(i)
            
            ## If the list is not []
            if prop_info[i] != None:
                            
                try:
                    
                    result = SQLtoUpdateTable(con, c, sql_statements, timestamp, prop_info[i])
                    print(len(result))
                
                    if result == []:
                        sql_statements = []
                    
                except:
                    print("ERROR: Error in python code trying to update property with ID {}".format(prop_info[i]['propertyId']))
                    #raise
                    next
                finally:
                    pass
        print("Called by TidyUpAll.  Commit changes to DB")
        ## Run the commands
        runSQLCommands(c, sql_statements, 'to commit changes to DB')

                
    

def variableExists(c, table, var):

    """
    Returns true if variable exists in table
    c  - SQL cursor
    table - string table name
    var = string, variable name
    """

    # Command - execute
    sql = """ PRAGMA table_info('{}');""".format(table)
    c.execute(sql)

    ## Get results
    res = c.fetchall()

    ## Create list of var names
    varnames = [var[1] for var in res]
    result = var in varnames
    return result


def AddColumn(dbname, table, columnName):

    con = sqlite3.connect(dbname)
    c = con.cursor()
    
    ## Property table - add lastFoundInSearch
    if not variableExists(c, table, columnName):
        sql = """ ALTER TABLE {}
                    ADD {} STRING
                """.format(table, columnName)
        c.execute(sql)
        con.commit()
    con.close()
        
        
    
def DeleteStringAdded(dbname):
    tables = ['location', 'images', 'features', 'property']

    sql = """ DELETE FROM {}
                WHERE propertyid IN
                    (SELECT DISTINCT propertyID
                        FROM property
                        WHERE addedOrReducedDate LIKE '%d%'
                    )
            """

    sqlcommands = []
    for t in tables:
        sqlcommands.append(sql.format(t))

    con = sqlite3.connect(dbname)
    c = con.cursor()
    for s in sqlcommands:
        c.execute(s)
        #print(s)
        con.commit()
        
def clearOldFilesWithPrefix(prefix='', ask=True):
    """
    Deletes all files in working folder which match the prefix given
    Where the last 8 characters (YYYYMMDD) are not today
    """
    files = os.listdir()

    ## All matches
    matches = [f for f in files if f[:len(prefix)]==prefix]

    # All matches - excluding today
    today = datetime.today().strftime('%Y%m%d')
    oldMatches = [m for m in matches if m[-len(today):]!=today]

    ## Delete all files
    [print("{}".format(i)) for i in oldMatches]
    if len(oldMatches)>0:
        if ask:
            check = input('Do you want to delete these {} file(s)?\nY/N? '.format(len(oldMatches))).upper()
        if not ask or check == 'Y':
            [os.unlink(f) for f in oldMatches]
            print("Files deleted.")
        

        return oldMatches


def LoadPostcodes(filename):

    """ Reads in .csv file delimited by comma and quote '"'
        and returns a list of postcodes and labels for the postcodes"""

    ## Init data to append to
    data = []
    
    ## Open .csv file    
    with open(filename, 'rt') as file:
        ## Get reader object to iterate over
        redear = csv.reader(file, dialect='excel', quotechar='"')

        ## Loop over each line in file
        for r in redear:
            data.append(r)
            #

        postcodes = [p[0] for p in data[1:]]
        labels = [p[1] for p in data[1:]]

    return postcodes, labels
    
    
def ReturnRandomSelection(seed, havelist, num):

    #
    ## Set seed 
    random.seed(seed)

    ## Create list of random numbers matching the size of the list
    indices = [i for i in range(len(havelist))]

    ## Celiing for largest number
    minnum = min(len(indices), num)
    
    #s
    ## Get random sample
    sample = random.sample(indices, minnum)
    
    ## Return these items
    final = [havelist[i] for i in sample]
    
    return final


   
def SelectRandomPostcodes2(seed, pcodesfile, dbname, num=1):

    """

        Updated 2019-02-05:

        Read in postcodes file (saved as a .csv)
        Check whether any postcodes are missing from the DB
        If so, these are prioritised
        If not, randomly select, the number (num) provided by user

        Changes:
            Brought back the logic to check the postcodes in the list against the DB
        
        
    """
    #pdb.set_trace()
    ## Get list of all postcodes from list
    postcodes, labels = LoadPostcodes(pcodesfile)

    ## Get list of postcode areas from DB
    con = sqlite3.connect(dbname)
    c = con.cursor()
    sql = """ SELECT DISTINCT SUBSTR(postcode, 1,INSTR(postcode, ' ')-1)
                FROM location"""

    ## Run query (with re-connection function)
    results = runSQLCommands(c, [sql], 'to select random postcodes', getResults=True)
    
    ## Isolate the interesting part of the postcode from the results
    results = [r[0] for r in results]

    ## Get list of postcodes with no data in DB
    #nodata_postcodes = [p for p in postcodes if p not in results]
    nodata_postcodes = results

    ## If user provided -1, return all postcodes
    if num == -1:
        return results
    else:
        ## Select random sample of postcodes from DB
        #sample = ReturnRandomSelection(seed, results, num)
        sample = ReturnRandomSelection(seed, nodata_postcodes, num)
            
        return sample
        
   
def SelectRandomPostcodes(seed, pcodesfile, dbname, num=1):

    """
        Read in postcodes file (saved as a .csv)
        Check whether any postcodes are missing from the DB
        If so, these are prioritised
        If not, randomly select, the number (num) provided by user
        
    """
    
    ## Get list of all postcodes from list
    #postcodes, labels = LoadPostcodes(pcodesfile)

    ## Get list of postcode areas from DB
    con = sqlite3.connect(dbname)
    c = con.cursor()
    sql = """ SELECT DISTINCT SUBSTR(postcode, 1,INSTR(postcode, ' ')-1)
                FROM location"""

    ## Run query (with re-connection function)
    results = runSQLCommands(c, [sql], 'to select random postcodes', getResults=True)
    
    ## Isolate the interesting part of the postcode from the results
    results = [r[0] for r in results]

    ## If user provided -1, return all postcodes
    if num == -1:
        return results
    else:
        ## Select random sample of postcodes from DB
        sample = ReturnRandomSelection(seed, results, num)
            
        return sample
        


def SelectRandomPostcodesOld(seed, pcodesfile, dbname, num=1):

    ### THIS HAS BEEN DECOMISSIONED BECAUSE SOME POSTCODES WERE
    ### NOT APPEARING AS THEY DID IN THE LIST
    ### e.g. SW1 actually was SW1A, SW1B, ...,
    ### DATE: March 2018
    ### Author: Andrew Craik

    """
        Read in postcodes file (saved as a .csv)
        Check whether any postcodes are missing from the DB
        If so, these are prioritised
        If not, randomly select, the number (num) provided by user
        
    """
    
    ## Get list of all postcodes from list
    postcodes, labels = LoadPostcodes(pcodesfile)

    ## Get list of postcode areas from DB
    con = sqlite3.connect(dbname)
    c = con.cursor()
    sql = """ SELECT DISTINCT SUBSTR(postcode, 1,INSTR(postcode, ' ')-1)
                FROM location"""

    ## Run query (with re-connection function)
    results = runSQLCommands(c, [sql], 'to select random postcodes', getResults=True)
    
    ## Isolate the interesting part of the postcode from the results
    results = [r[0] for r in results]

    ## Get list of postcodes with no data in DB
    nodata_postcodes = [p for p in postcodes if p not in results]
    
    ## If 'nodata' list is null then write the list out to a file
    if nodata_postcodes == []:

        print("All postcodes in the .csv file '{}' are in the DB.\nSelecting {} of them.".format(pcodesfile), num)
        ## Randomly select from results
        sample = ReturnRandomSelection(seed, results, num)
    
    ## Else, select a random set of postcodes from list
    else:

        ## Randomly select from (hopefully decreasing list of postcodes NOT in DB
        print("There are {} postcodes in the .csv file which are not in the DB\nWill select {} of them".format(len(nodata_postcodes), num))
        sample = ReturnRandomSelection(seed, nodata_postcodes, num)
        
        
    
    return sample
    
    pass


## Create DB tables
if __name__ == '__main__':
    pass
    
    #SelectRandomPostcodes(seed, pcodes, dbname, 2)




