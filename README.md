## FPLScraper

Scrapes from Understat and the FPL API and combines player data into one table. Outputs to DOC, CSV and SQLITE.

Run at the end of each gameweek to have more granular statistics, showing player data through each gameweek. Running once will return the players total statistics up until the current gameweek.

## Installation

requirements.txt contains the libraries required to run the program.

More details about the understat package can be found here: 

https://understat.readthedocs.io/en/latest/index.html

## Usage

Running the program as is will scrape the FPL API and Understat for player data. In subsequent transfer windows, the FPL player base will change as players move clubs. As both databases do not always call players by the same name (accents, full name vs shortened, etc), the name_change and manual_name_change dictionaries may need to be altered manually. Most of the names should be matched automatically using fuzzy string matching, however, some player names can be missed.

## Licence

[MIT](https://choosealicense.com/licenses/mit/)
