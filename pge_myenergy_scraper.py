# Matt Duesterberg
# PG&E Scraper
# 9/19/2013
# Scraper to login into PG&E's website, extract the green button data and store it in a local database
# -*- coding: utf-8 -*-

from lib import *
import urllib2
import urllib
import json
import time
import cookielib
from BeautifulSoup import BeautifulSoup
from HTMLParser import HTMLParser
import zipfile
import csv

# Login information that needs to be added in at the beginning
login_name = 'XXXXX'
login_password = 'XXXX'

# Class built to navigate through PG&E browser interaction
class PgeInteract(object):
    # In starting to login to PG&E, there is three links that need to be hit
    def __init__(self, login, password, bln_log = False):
        """ Start up... """
        self.login = login
        self.password = password
        self.bln_log = bln_log
        
        self.directory = 'XXXXXXXXXX'
                
        cookie_filename = '%s/pge.cookies' % (self.directory)
        self.cj = cookielib.MozillaCookieJar(cookie_filename)
        if os.access(cookie_filename, os.F_OK):
            self.cj.load()
        self.opener = urllib2.build_opener(
            urllib2.HTTPRedirectHandler(),
            urllib2.HTTPHandler(debuglevel=0),
            urllib2.HTTPSHandler(debuglevel=0),
            urllib2.HTTPCookieProcessor(self.cj)
        )
        self.opener.addheaders = [('User-agent', ('Mozilla/5.0 (Windows NT 6.1; WOW64)'))]
        
        # Need to login to the public site (HTTP) first, which automatically pushes this data over to the private site (HTTPs)
        response_read = self.loginToHttp()
        posted_inputs = self.extractHiddenInputs(response_read)
        # need this twice - once to set cookies, once to log in...
        self.loginToHttps(posted_inputs)
        self.loginToHttps(posted_inputs)
        self.cj.save(ignore_discard = True, ignore_expires = True)
    
    def saveDownloads(self, full_file, file_type = 'html'):
        if self.bln_log == True:
            now = datetime.datetime.now()
            filename = '%s/pge_attempt_%s.%s' % (self.directory, format(now, "%H%M%S"), file_type)
            f = open(filename, 'w+')
            f.write(full_file)
            f.close()
        return 1
    
    def extractHiddenInputs(self, full_file): 
        soup = BeautifulSoup(full_file)
        attribs = soup.findAll('input')
        post_values = {}
        for attrib in attribs:
            if attrib['type'] == 'hidden':
                post_values[attrib['name']] = attrib['value']
        return post_values
    
    def loginToHttp(self):
        # Logging into the public facing site (HTTP)
        login_data = urllib.urlencode({
            'USER' : self.login,
            'PASSWORD' : self.password            
        })                
        response = self.opener.open("http://www.pge.com/myenergy", login_data) 
        file_response = response.read()
        self.saveDownloads(file_response)
        return file_response
    
    def loginToHttps(self, inputs):
        # Logging into the private site with the headers from the HTTPS
        inputs['USER'] = self.login
        inputs['PASSWORD'] = self.password
        login_data = urllib.urlencode(inputs)                
        response = self.opener.open("https://www.pge.com/eum/login", login_data) 
        file_response = response.read()
        self.saveDownloads(file_response)
        return file_response
    
    # We need to go through 3 different links to move login data from PG&E over to oPower in order to login cleanly
    def getOpowerLogin(self):
        response = self.opener.open('https://www.pge.com/myenergyweb/appmanager/pge/customer?_nfpb=true&_pageLabel=MyUsage&_nfls=false')
        file_response = response.read()
        self.saveDownloads(file_response)
        opower_values = self.extractHiddenInputs(file_response)
        opower_soup = BeautifulSoup(file_response)
        opower_values['method'] = 'POST'
        opower_values['SUBMIT'] = 'Continue'
        
        response = self.opener.open(str(opower_soup.form['action']), urllib.urlencode(opower_values)) 
        file_response = response.read()
        self.saveDownloads(file_response)
        opower_values = self.extractHiddenInputs(file_response)
        opower_values['method'] = 'POST'
        opower_values['submit'] = 'Resume'
        
        self.opener.addheaders = [
            ('Host', ('pge.opower.com')),
            ('Origin', ('https://sso.opower.com')),
            ('Referrer', ('https://sso.opower.com/sp/ACS.saml2')),
            ('User-Agent', ('Mozilla/5.0 (Windows NT 6.1; WOW64)'))
        ]
        return opower_values
    
    def close(self):
        self.opener.open("https://pge.opower.com/ei/app/logout")
        return 1


    
log = fin_log(os.path.basename( __file__ ), verbose = True)
script_desc = 'Downloading all PGE data from Green Button for single user'
log.write(script_desc)
dbConn = connectTo('access')
conn = dbConn.cursor(cursor_class=MySQLCursorDict)

saveTable = 'projects.pge_cons'
billTable = 'projects.pge_bills'

conn.execute('''drop table %s''' % (billTable))
conn.execute('''drop table %s''' % (saveTable))
conn.execute('''create table %s (account_id int unsigned, start_dt date, end_dt date, usage_type varchar(128), name varchar(255), address varchar(255),
	primary key (account_id, start_dt, usage_type))''' % (billTable))
conn.execute('''create table %s (account_id int unsigned, dttm datetime, usage_type varchar(128), cons double, cost double,
	primary key (account_id, dttm, usage_type))''' % (saveTable))

log.write("Initializing login to PGE with login information")
pge_browser = PgeInteract(login_name, login_password, bln_log = False)   

log.write("Transferring cookie information over to OPower for access to OPower database")
opower_values = pge_browser.getOpowerLogin()

# Getting the first set of information (bills, etc)
log.write("Getting the amount of bills we have data for")
response = pge_browser.opener.open('https://pge.opower.com/ei/app/myEnergyUse', urllib.urlencode(opower_values))
bill_response = response.read()
customer_info = bill_response[bill_response.find('<a data-trigger-dialog'):bill_response.find('</a>', bill_response.find('<a data-trigger-dialog'))]
green_button_lnk = customer_info[customer_info.find('href="')+6:customer_info.find('"', customer_info.find('href="')+6)]
green_button_lnk_fields = green_button_lnk.split("/")
customer_id = green_button_lnk_fields[5]
log.write("Found customer ID (%s) from green button link fields" % (customer_id))

# Now getting all the data available from the green button link
response = pge_browser.opener.open('https://pge.opower.com%s' % (green_button_lnk)) 
app_access = response.read()
gb_soup = BeautifulSoup(app_access)
gb_mnths = gb_soup.select.findAll('option')
log.write("Got a total of %s months to parse through" % (len(gb_mnths)))

arrBills = []
arrInserts = []
cnt_mnths = 0
for gb_mnth in gb_mnths:
    cnt_mnths += 1
    bill_value = gb_mnth['value']
    gb_str = gb_mnth.string.replace("\n", "").replace("Since your last bill:", "")
    arrEnds = gb_str.split("&ndash;")
    start = datetime.datetime.strptime(arrEnds[0].replace(" ", ""), "%b%d,%Y")
    end = datetime.datetime.strptime(arrEnds[1].replace(" ", ""), "%b%d,%Y")

    response = pge_browser.opener.open('''https://pge.opower.com/ei/app/modules/customer/%s/energy/download?exportFormat=CSV_AMI_SINGLE_BILL&bill=%s&csvFrom=%s&csvTo=%s''' % (customer_id, \
       bill_value, format(start, "%m_%d_%Y").replace("_", "%2F"), format(end, "%m_%d_%Y").replace("_", "%2F")))        
    app_access = response.read()
        
    # Save the file as a zip file
    filename = '%s/zipped_data_%s.zip' % (pge_browser.directory, format(start, "%Y%m%d"))
    f = open(filename, 'wb')
    f.write(app_access)
    f.close()

    # Unzip the file
    zfile = zipfile.ZipFile(filename)
    zfile.extractall(pge_browser.directory)
    
    # Going through the zipped file to extract out the csvs and scrape the data
    for gb_file in zfile.namelist():
        filename = pge_browser.directory + "/" + gb_file
        csv_reader = csv.reader(open(filename, 'r'))

        usage_type = gb_file[gb_file.find("Daily")+5:gb_file.find("Usage")]
        
        # Top three lines of the CSV (name, addy, and account_num)-
        line = csv_reader.next()
        name = line[1]
        line = csv_reader.next()
        address = line[1]
        line = csv_reader.next()
        account_num = line[1]
        
        arrBills.append([account_num, start, end, usage_type, name, address])
        
        # Simple CSV scraper that goes through each line
        row_id = 0
        for line in csv_reader:
            row_id += 1
            if len(line) > 6 and line[0] != "TYPE" and usage_type == 'Electric':
                arrInserts.append([account_num, datetime.datetime.strptime(line[1] + " " + line[2], "%Y-%m-%d %H:%M"), usage_type, line[4], line[6].replace("$", "")])
            elif len(line) > 3 and line[0] != "TYPE" and usage_type == 'NaturalGas':
                arrInserts.append([account_num, datetime.datetime.strptime(line[1] + " 00:00", "%Y-%m-%d %H:%M"), usage_type, line[2], line[4].replace("$", "")])
            
    if len(arrInserts) > 0:
        conn.executemany("insert into %s values (%s)" % (saveTable, ','.join([ '%s' ] * len(arrInserts[0]))), arrInserts)
        arrInserts = []
        dbConn.commit()
        log.getExpectedEndTime(cnt_mnths, len(gb_mnths))  
        
if len(arrBills) > 0:
    conn.executemany("insert into %s values (%s)" % (billTable, ','.join([ '%s' ] * len(arrBills[0]))), arrBills)

pge_browser.close()  
    
log.write("Finished PGE pull for user %s" % (pge_browser.login))

dbConn.commit()
log.close()
dbConn.close()

