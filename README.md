# rmove-scraper
Andrew Craik, February 2018


## Depdendencies
BeautifulSoup
sqlite3

## Property Scraper

Program for extracting data from a well known property website.
The program will loop through a random sample of postcodes.
After returning basic information from the search index pages, it will then check the main advertising listing.
If the property doesn't exist already, it will add it.  If it already exists, it will only check if the listing has been changed.

## Postcodes

Postcodes should be saved in a folder in the main repository called "master_postcode_lists"
