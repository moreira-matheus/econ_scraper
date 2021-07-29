#!/bin/bash
. venv/bin/activate
cd webscraping
scrapy crawl $1
cd ..
deactivate
