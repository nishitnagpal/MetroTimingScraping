# Kolkata Metro Rail Web Scraping 🚇
This project focuses on web scraping the official Kolkata Metro Rail website to obtain real-time train schedules for all available services, routes, and stations.

## 🌐 Overview
Using Python Scrapy, we collect metro timing charts directly from the official website. The data is routinely scraped to ensure that the timings are always up-to-date and accurate.

The scraped timing data is processed into a well-structured format and refined into a JSON file. This file powers the Kolkata Metro Rail webpage, allowing users to easily access the next available train timings from their selected station and route.

## 🛠️ Key Features
Automated Scraping: The script runs routinely to scrape the official Kolkata Metro website, keeping the timing data fresh.
All Routes & Stations Covered: Timings for all metro services, routes, and stations are captured.
Processed Data: The scraped data is converted into lists and further refined into a structured JSON file.
Real-time Train Timings: The website displays the next available train for the selected station, based on current time and date.

## 🚀 Links                      
Project Repo: [Kolkata Metro Rail Schedule] (https://github.com/nishitnagpal/KolkataMetroTimings)
Live Website: [Kolkata Metro Timings] (https://kolkata-metro-timings.vercel.app/)

## 🤖 How It Works
Scraping: The Python Scrapy spider scrapes the official website’s tables to fetch metro timings for all services and stations.
Processing: The scraped data is processed into lists, restructured, and refined into a JSON format.
Usage: This JSON file is used by the Kolkata Metro Rail webpage to display the next available train timing for the selected station.

### 🚨 Note: The scraper runs regularly to ensure that the most accurate and updated data is available.

Feel free to explore the code, contribute to the project, or simply use the website to stay on track with Kolkata Metro timings!