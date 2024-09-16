import scrapy
from datetime import datetime, timedelta

class MetroSpider(scrapy.Spider):
    name = 'metro_spider'
    start_urls = ['https://mtp.indianrailways.gov.in/view_section.jsp?lang=0&id=0,2,630']

    def parse(self, response):

        lines = response.xpath('//table[@id="table18"]//tr//td[3]//a[contains(@href, "view_section.jsp")]/@href').getall()
        line_colors = response.xpath('//table[@id="table18"]//tr//td[3]//a[contains(@href, "view_section.jsp")]/text()').getall()

        raw_line_data = {line_colors[i]: response.urljoin(lines[i]) for i in range(len(lines))}
        line_data = {key.replace('\r', '').replace('\n', '').replace('\t', '').strip(): value for key, value in raw_line_data.items()}

        #Follow each link to scrape the timetable for each line
        for color, href in line_data.items():
            yield response.follow(href, callback=self.parse_timetable, meta={'line_color': color})

    def parse_timetable(self, response):
        # Extract the line color from the meta data
        line_color = response.meta['line_color']
    
        days_of_week = ['Weekday', 'Saturday', 'Sunday']
        #weekday = ''.join(weekday).strip()


        # Processing based on line color
        if line_color.lower() == 'north-south (blue line)':
            
            for i, day in enumerate(days_of_week):
                # Select the table based on its index
                table = response.xpath(f'//table[@class="MsoNormalTable"][{i+1}]')

                # Extract UP direction of travel from the row with style "mso-yfti-irow:1" 
                direction_uptravel = response.xpath('//table[@class="MsoNormalTable"][1]//tr[contains(@style, "mso-yfti-irow:2")]//td[1]//p[@class="MsoNormal"]/b/span/text()').getall()
                direction_uptravel = ''.join(direction_uptravel).strip()

                # Extract DN direction of travel from the row with style "mso-yfti-irow:1" 
                direction_dntravel = response.xpath('//table[@class="MsoNormalTable"][1]//tr[contains(@style, "mso-yfti-irow:2")]//td[3]//p[@class="MsoNormal"]/b/span/text()').getall()
                direction_dntravel = ''.join(direction_dntravel).strip()

                # Select all rows starting from the third row
                rows = table.xpath('.//tr[starts-with(@style, "mso-yfti-irow")]')[3:]  # Skip the first three rows
                    
                dep_kavi_subhash_up = []
                dep_dum_dum_up = []
                arr_dakhineshwar_up = []
                dep_dakhineshwar_dn = []
                dep_dum_dum_dn = []
                arr_kavi_subhash_dn = []
                    
                # Initialize a dictionary to hold station timings
                stations_up_first = {
                    'Shahid Khudiram (up)': 3, 'Kavi Nazrul (up)': 2, 'Gitanjali (up)': 2, 'Masterda Suryasen (up)': 2, 
                    'Netaji (up)': 3, 'Mahanayak Uttam Kumar (up)': 3, 'Rabindra Sarobar Up': 2, 'Kalighat (up)': 2, 
                    'Jatin Das Park (up)': 2, 'Netaji Bhavan (up)': 2, 'Rabindra Sadan (up)': 2, 'Maidan (up)': 2, 
                    'Park Street (up)': 1, 'Esplanade (up)': 2, 'Chandni Chowk (up)': 1, 'Central (up)': 2, 
                    'M.G. Road (up)': 2, 'Girish Park (up)': 2, 'Shobhabazar Sutanuti (up)': 2, 'Shyambazar (up)': 2, 
                    'Belgachia (up)': 3
                }

                stations_up_second = {
                    'Noapara (up)': 5, 'Baranagar (up)': 6
                }

                stations_dn_first = {
                    'Belgachia (dn)': 3, 'Shyambazar (dn)': 2, 'Shobhabazar Sutanuti (dn)': 2, 'Girish Park (dn)': 2, 
                    'M.G. Road (dn)': 2, 'Central (dn)': 2, 'Chandni Chowk (dn)': 2, 'Esplanade (dn)': 1, 'Park Street (dn)': 2,
                    'Maidan (dn)': 1, 'Rabindra Sadan (dn)': 2, 'Netaji Bhavan (dn)': 2, 'Jatin Das Park (dn)': 2, 
                    'Kalighat (dn)': 2, 'Rabindra Sarobar (dn)': 2, 'Mahanayak Uttam Kumar (dn)': 3, 'Netaji (dn)': 4,
                    'Masterda Suryasen (dn)': 3, 'Gitanjali (dn)': 2, 'Kavi Nazrul (dn)': 2, 'Shahid Khudiram (dn)': 2      
                }

                stations_dn_second = {
                    'Baranagar (dn)': 6, 'Noapara (dn)': 5 
                }

                # Initialize the timing lists within the dictionary
                station_timings_up_first = {station: [] for station in stations_up_first.keys()}
                station_timings_up_second = {station: [] for station in stations_up_second.keys()}

                station_timings_dn_first = {station: [] for station in stations_dn_first.keys()}
                station_timings_dn_second = {station: [] for station in stations_dn_second.keys()}

                # Function to process timings
                '''
                def process_timings(base_timing, check_timing, stations, station_timings):
                    increment = 0
                    for station, up_increment in stations.items():
                        if check_timing != "X" and check_timing != "":
                            increment += up_increment
                            timing = (datetime.strptime(base_timing, '%H:%M') + timedelta(minutes=increment)).strftime('%H:%M')
                            station_timings[station].append(timing)
                        else:
                            station_timings[station].append("X")
                '''

                # Function to process timings for stations after "Mahanayak Uttam Kumar"
                def process_timings(base_timing, check_timing, stations, station_timings, state, start_station):
                    increment = 0
                    if state == True:
                        station_list = list(stations.keys())
                        start_index = station_list.index(start_station)
                        for station in station_list[start_index + 1:]:
                            up_increment = stations[station]
                            if base_timing != "X" and base_timing != "":
                                increment += up_increment
                                timing = (datetime.strptime(base_timing, '%H:%M') + timedelta(minutes=increment)).strftime('%H:%M')
                                station_timings[station].append(timing)
                    else:              
                        for station, up_increment in stations.items():
                            if check_timing != "X" and check_timing != "":
                                increment += up_increment
                                timing = (datetime.strptime(base_timing, '%H:%M') + timedelta(minutes=increment)).strftime('%H:%M')
                                station_timings[station].append(timing)
                            else:
                                station_timings[station].append("X")

                for row in rows:
                # Extract the text from each cell in the row    
                    if day == 'Weekday':
                        dep_kavi_subhash_up_timing = ''.join(row.xpath('.//td[1]//p[@class="MsoNormal"]/text()').getall()).strip()
                        dep_kavi_subhash_up.append(dep_kavi_subhash_up_timing)

                        dep_dum_dum_up_timing = ''.join(row.xpath('.//td[3]//p[@class="MsoNormal"]/text()').getall()).strip()
                        dep_dum_dum_up.append(dep_dum_dum_up_timing)

                        arr_dakhineshwar_up_timing= ''.join(row.xpath('.//td[4]//p[@class="MsoNormal"]/text()').getall()).strip()
                        arr_dakhineshwar_up.append(arr_dakhineshwar_up_timing)

                        # Extract the timing for "Mahanayak Uttam Kumar (up)" for Weekday
                        mahanayak_uttam_kumar_up_timing = ''.join(row.xpath('.//td[2]//p[@class="MsoNormal"]/text()').getall()).strip()
                        if mahanayak_uttam_kumar_up_timing != '' and mahanayak_uttam_kumar_up_timing != "":
                            station_timings_up_first['Mahanayak Uttam Kumar (up)'].append(mahanayak_uttam_kumar_up_timing)

                        # Calculate timings for stations from "Mahanayak Uttam Kumar (up)" onwards
                        process_timings(mahanayak_uttam_kumar_up_timing, mahanayak_uttam_kumar_up_timing, stations_up_first, station_timings_up_first, True, 'Mahanayak Uttam Kumar (up)')

                    else:    
                        dep_kavi_subhash_up_timing = ''.join(row.xpath('.//td[1]//p[@class="MsoNormal"]/span/text()').getall()).strip()
                        dep_kavi_subhash_up.append(dep_kavi_subhash_up_timing)

                        dep_dum_dum_up_timing = ''.join(row.xpath('.//td[2]//p[@class="MsoNormal"]/span/text()').getall()).strip()
                        dep_dum_dum_up.append(dep_dum_dum_up_timing)

                        arr_dakhineshwar_up_timing= ''.join(row.xpath('.//td[3]//p[@class="MsoNormal"]/span/text()').getall()).strip()
                        arr_dakhineshwar_up.append(arr_dakhineshwar_up_timing)
                    
                    # Process timings for the first set of stations
                    process_timings(dep_kavi_subhash_up_timing, dep_kavi_subhash_up_timing, stations_up_first, station_timings_up_first, False, None)

                    # Process timings for the second set of stations
                    process_timings(dep_dum_dum_up_timing, arr_dakhineshwar_up_timing, stations_up_second, station_timings_up_second, False, None)

                    if day == 'Weekday':
                        dep_dakhineshwar_dn_timing = ''.join(row.xpath('.//td[5]//p[@class="MsoNormal"]/text()').getall()).strip()
                        dep_dakhineshwar_dn.append(dep_dakhineshwar_dn_timing)
                            
                        dep_dum_dum_dn_timing = ''.join(row.xpath('.//td[6]//p[@class="MsoNormal"]/text()').getall()).strip()
                        dep_dum_dum_dn.append(dep_dum_dum_dn_timing)
                            
                        arr_kavi_subhash_dn_timing = ''.join(row.xpath('.//td[7]//p[@class="MsoNormal"]/text()').getall()).strip()
                        arr_kavi_subhash_dn.append(arr_kavi_subhash_dn_timing)
                    else:
                        dep_dakhineshwar_dn_timing = ''.join(row.xpath('.//td[4]//p[@class="MsoNormal"]/span/text()').getall()).strip()
                        dep_dakhineshwar_dn.append(dep_dakhineshwar_dn_timing)
                            
                        dep_dum_dum_dn_timing = ''.join(row.xpath('.//td[5]//p[@class="MsoNormal"]/span/text()').getall()).strip()
                        dep_dum_dum_dn.append(dep_dum_dum_dn_timing)
                            
                        arr_kavi_subhash_dn_timing = ''.join(row.xpath('.//td[6]//p[@class="MsoNormal"]/span/text()').getall()).strip()
                        arr_kavi_subhash_dn.append(arr_kavi_subhash_dn_timing)
                    # Process timings for the first set of stations
                    process_timings(dep_dum_dum_dn_timing, arr_kavi_subhash_dn_timing, stations_dn_first, station_timings_dn_first, False, None)

                    # Process timings for the second set of stations
                    process_timings(dep_dakhineshwar_dn_timing, dep_dakhineshwar_dn_timing, stations_dn_second, station_timings_dn_second, False, None)
        
                    # Yield the extracted data
                yield {
                    'color': 'blue',
                    'line': line_color,
                    'day': day,
                    'direction (up)': direction_uptravel,
                    'Kavi Subhash (up)': dep_kavi_subhash_up,
                    **station_timings_up_first,
                    'Dum Dum (up)': dep_dum_dum_up,
                    **station_timings_up_second,
                    'Dakhineshwar (up)': arr_dakhineshwar_up,
                    'direction (dn)': direction_dntravel,
                    'Dakhineshwar (dn)': dep_dakhineshwar_dn,
                    **station_timings_dn_second,
                    'Dum Dum (dn)': dep_dum_dum_dn,
                    **station_timings_dn_first,
                    'Kavi Subhash (dn)': arr_kavi_subhash_dn,
                }     
                   
        if line_color.lower() == 'east - west (green line)':

            for i, day in enumerate(days_of_week):

                if(day == 'Saturday'):
                    # Extract the tables for up and down directions
                    table_up = response.xpath('//table[@class="MsoNormalTable" and @border="1"][1]')
                    table_dn = response.xpath('//table[@class="MsoNormalTable" and @border="1"][2]')

                    # Extract station names from the header rows
                    station_up = table_up.xpath('.//tr[2]//td//p[@class="MsoNormal"]//b/span/text()').getall()
                    station_dn = table_dn.xpath('.//tr[2]//td//p[@class="MsoNormal"]//b/span/text()').getall()

                    # Extract the rows containing the train timings
                    rows_up = table_up.xpath('.//tr[position() >= 3]')
                    rows_dn = table_dn.xpath('.//tr[position() >= 3]')

                    def extract_timings(rows, stations, target_station):
                        station_timings = []
                        for row in rows:
                            for j, station in enumerate(stations):
                                if station == target_station:
                                    # Extract timing for the target station
                                    cell_data = row.xpath(f'.//td[{j+1}]//p[@class="MsoNormal"]//span/text() | .//td[{j+1}]//p[@class="MsoNormal"]//b/span/text()').get()
                                    station_timings.append(cell_data)
                        return station_timings

                    # Initialize lists to store the timings for Sealdah
                    station_SDHM_up = extract_timings(rows_up, station_up, 'SEALDAH (SDHM)')
                    station_SDHM_dn = extract_timings(rows_dn, station_dn, 'SEALDAH (SDHM)')
                    station_PBGB_up = extract_timings(rows_up, station_up, 'PHOOL BAGAN (PBGB)')
                    station_PBGB_dn = extract_timings(rows_dn, station_dn, 'PHOOL BAGAN (PBGB)')
                    station_SSSA_up = extract_timings(rows_up, station_up, 'SALT LAKE STADIUM (SSSA)')
                    station_SSSA_dn = extract_timings(rows_dn, station_dn, 'SALT LAKE STADIUM (SSSA)')
                    station_BCSD_up = extract_timings(rows_up, station_up, 'BENGAL CHEMICAL (BCSD)')
                    station_BCSD_dn = extract_timings(rows_dn, station_dn, 'BENGAL CHEMICAL (BCSD)')
                    station_CCSC_up = extract_timings(rows_up, station_up, 'CITY CENTER (CCSC)')
                    station_CCSC_dn = extract_timings(rows_dn, station_dn, 'CITY CENTER (CCSC)')
                    station_CPSA_up = extract_timings(rows_up, station_up, 'CENTRAL PARK (CPSA)')
                    station_CPSA_dn = extract_timings(rows_dn, station_dn, 'CENTRAL PARK (CPSA)')
                    station_KESA_up = extract_timings(rows_up, station_up, 'KARUNA MOYEE (KESA)')
                    station_KESA_dn = extract_timings(rows_dn, station_dn, 'KARUNA MOYEE (KESA)')
                    station_SVSA_up = extract_timings(rows_up, station_up, 'SALT LAKE SECTOR V (SVSA)')
                    station_SVSA_dn = extract_timings(rows_dn, station_dn, 'SALT LAKE SECTOR V (SVSA)')

                    table = response.xpath('//table[@class="MsoNormalTable"][1]')
                    rows = table.xpath('.//tr[contains(@style, "mso-yfti-irow")]')

                    station_data = {
                        'HWMM': {'up': [], 'dn': [], 'is_up': True},
                        'HWHM': {'up': [], 'dn': [], 'is_up': True},
                        'MKNA': {'up': [], 'dn': [], 'is_up': True},
                        'KESP': {'up': [], 'dn': [], 'is_up': True},    
                    }    
                    
                    is_up_direction = True

                    def process_row(row, station_key, is_up_direction):
                        for cell in row.xpath('.//td'):
                            cell_data = cell.xpath('.//p[@class="MsoNormal"]//span/text() | .//p[@class="MsoNormal"]//b/span/text()').get()
                            if cell_data is not None and cell_data != station_key:       
                                if is_up_direction:
                                    station_data[station_key]['up'].append(cell_data)
                                else:
                                    station_data[station_key]['dn'].append(cell_data)   

                    for row in rows:
                        direction_text = ' '.join(row.xpath('.//td[1]//p[@class="MsoNormal"]//b/span/text()').getall()).strip()
                        if 'EAST' in direction_text:  
                            is_up_direction = True
                        elif 'WEST' in direction_text:
                            is_up_direction = False

                        station = row.xpath('.//td[1]//b/span/text()').get() 
                        if station and station.strip() in station_data:
                            process_row(row, station.strip(), is_up_direction)

                    # Yield the station data as a dictionary
                    yield{
                        'color': 'green',
                        'line': line_color,
                        'day': day,
                        'Sealdah (up)': station_SDHM_up,
                        'Sealdah (dn)': station_SDHM_dn,
                        'Phool Bagan (up)': station_PBGB_up,
                        'Phool Bagan (dn)': station_PBGB_dn,
                        'Salt Lake Stadium (up)': station_SSSA_up,
                        'Salt Lake Stadium (dn)': station_SSSA_dn,
                        'Bengal Chemical (up)': station_BCSD_up,
                        'Bengal Chemical (dn)': station_BCSD_dn,
                        'City Centre (up)': station_CCSC_up,
                        'City Centre (dn)': station_CCSC_dn,
                        'Central Park (up)': station_CPSA_up,
                        'Central Park (dn)': station_CPSA_dn,
                        'Karunamoyee (up)': station_KESA_up,
                        'Karunamoyee (dn)': station_KESA_dn,
                        'Salt Lake Sector-V (up)': station_SVSA_up,
                        'Salt Lake Sector-V (dn)': station_SVSA_dn,
                        'Howrah Maidan (up)': station_data['HWMM']['up'],
                        'Howrah (up)': station_data['HWHM']['up'],
                        'New Mahakaran (up)': station_data['MKNA']['up'],
                        'Esplanade (up)': station_data['KESP']['up'],
                        'Howrah Maidan (dn)': station_data['HWMM']['dn'],
                        'Howrah (dn)': station_data['HWHM']['dn'],
                        'New Mahakaran (dn)': station_data['MKNA']['dn'],
                        'Esplanade (dn)': station_data['KESP']['dn'],     
                    }   
                elif (day == 'Weekday'):
                    table = response.xpath('//table[@class="MsoNormalTable"][1]')
                    rows = table.xpath('.//tr[contains(@style, "mso-yfti-irow")]')

                    station_data = {
                    'SDHM': {'up': [], 'dn': [], 'is_up': True},
                    'PBGB': {'up': [], 'dn': [], 'is_up': True},
                    'SSSA': {'up': [], 'dn': [], 'is_up': True},
                    'BCSD': {'up': [], 'dn': [], 'is_up': True},
                    'CCSC': {'up': [], 'dn': [], 'is_up': True},
                    'CPSA': {'up': [], 'dn': [], 'is_up': True},
                    'KESA': {'up': [], 'dn': [], 'is_up': True},
                    'SVSA': {'up': [], 'dn': [], 'is_up': True},
                    'HWMM': {'up': [], 'dn': [], 'is_up': True},
                    'HWHM': {'up': [], 'dn': [], 'is_up': True},
                    'MKNA': {'up': [], 'dn': [], 'is_up': True},
                    'KESP': {'up': [], 'dn': [], 'is_up': True},
                    }

                    
                    def process_row(row, station_key):
                        for cell in row.xpath('.//td'):
                            cell_data = cell.xpath('.//p[@class="MsoNormal"]//span/text() | .//p[@class="MsoNormal"]//b/span/text()').get()
                            if cell_data is not None and cell_data != station_key:
                                # Check for transition point from 'up' to 'down'
                                if cell_data == '-' and station_data[station_key]['is_up']:
                                    station_data[station_key]['is_up'] = False

                                # Append data to the appropriate list
                                if station_data[station_key]['is_up']:
                                    station_data[station_key]['up'].append(cell_data)
                                else:
                                    if cell_data != '-':
                                        station_data[station_key]['dn'].append(cell_data)

                    for row in rows:
                        station = row.xpath('.//td[1]//b/span/text()').get()

                        if station and station.strip() in station_data:
                            process_row(row, station.strip())
                    
                    is_up_direction = True

                    def process_row_2(row, station_key, is_up_direction):
                        for cell in row.xpath('.//td'):
                            cell_data = cell.xpath('.//p[@class="MsoNormal"]//span/text() | .//p[@class="MsoNormal"]//b/span/text()').get()
                            if cell_data is not None and cell_data != station_key:       
                                if is_up_direction:
                                    station_data[station_key]['up'].append(cell_data)
                                else:
                                    station_data[station_key]['dn'].append(cell_data)   

                    for row in rows:
                        direction_text = ' '.join(row.xpath('.//td[1]//p[@class="MsoNormal"]//b/span/text()').getall()).strip()
                        if 'EAST' in direction_text:  
                            is_up_direction = True
                        elif 'WEST' in direction_text:
                            is_up_direction = False

                        station = row.xpath('.//td[1]//b/span/text()').get() 
                        if station and station.strip() in station_data:
                            process_row_2(row, station.strip(), is_up_direction)

                    # Yield the station data as a dictionary
                    yield{
                        'color': 'green',
                        'line': line_color,
                        'day': day,
                        'Sealdah (up)': station_data['SDHM']['up'],
                        'Sealdah (dn)': station_data['SDHM']['dn'],
                        'Phool Bagan (up)': station_data['PBGB']['up'],
                        'Phool Bagan (dn)': station_data['PBGB']['dn'],
                        'Salt Lake Stadium (up)': station_data['SSSA']['up'],
                        'Salt Lake Stadium (dn)': station_data['SSSA']['dn'],
                        'Bengal Chemical (up)': station_data['BCSD']['up'],
                        'Bengal Chemical (dn)': station_data['BCSD']['dn'],
                        'City Centre (up)': station_data['CCSC']['up'],
                        'City Centre (dn)': station_data['CCSC']['dn'],
                        'Central Park (up)': station_data['CPSA']['up'],
                        'Central Park (dn)': station_data['CPSA']['dn'],
                        'Karunamoyee (up)': station_data['KESA']['up'],
                        'Karunamoyee (dn)': station_data['KESA']['dn'],
                        'Salt Lake Sector-V (up)': station_data['SVSA']['up'],
                        'Salt Lake Sector-V (dn)': station_data['SVSA']['dn'],
                        'Howrah Maidan (up)': station_data['HWMM']['up'],
                        'Howrah (up)': station_data['HWHM']['up'],
                        'New Mahakaran (up)': station_data['MKNA']['up'],
                        'Esplanade (up)': station_data['KESP']['up'],
                        'Howrah Maidan (dn)': station_data['HWMM']['dn'],
                        'Howrah (dn)': station_data['HWHM']['dn'],
                        'New Mahakaran (dn)': station_data['MKNA']['dn'],
                        'Esplanade (dn)': station_data['KESP']['dn'],     
                    }  
                else:
                    station_HWMM_up = ['UP','D','14:15:00','14:40:00','15:00:00','15:20:00','15:40:00','16:00:00','16:20:00','16:40:00','17:00:00','17:20:00','17:40:00','18:00:00','18:20:00','18:40:00','D','19:00:00','19:20:00','19:40:00','20:00:00','20:20:00','20:40:00','21:00:00','21:20:00','21:40:00']
                    station_KESP_dn = ['DN','D','14:30:00','14:50:00','15:10:00','15:30:00','15:50:00','16:10:00','16:30:00','16:50:00','17:10:00','17:30:00','17:50:00','18:10:00','18:30:00','18:50:00','D','19:10:00','19:30:00','19:50:00','20:10:00','20:30:00','20:50:00','21:10:00','21:30:00','21:50:00']    
                
                    stations_up = {
                        'Howrah (up)': (2,30), 'New Mahakaran (up)': (3,30), 'Esplanade (up)': (2,0)
                    } 

                    stations_dn = {
                        'New Mahakaran (dn)': (2,30), 'Howrah (dn)': (3,30), 'Howrah Maidan (dn)': (2,0)
                    }

                    station_timings_up = {station: [] for station in stations_up.keys()}
                    station_timings_dn = {station: [] for station in stations_dn.keys()}
                    
                    def process_timings(base_timing, stations, station_timings):
                        current_time = []
                        for timing in base_timing:
                            increment_minutes = 0
                            increment_seconds = 0
                            if timing != 'UP' and timing != 'D' and timing != 'DN' and timing != 'A':
                                current_time = datetime.strptime(timing, '%H:%M:%S')        
                                
                                for station, (minutes, seconds) in stations.items():
                                    increment_minutes += minutes
                                    increment_seconds += seconds
                                    formatted_time = (current_time + timedelta(minutes=increment_minutes, seconds=increment_seconds)).strftime('%H:%M:%S')
                                    station_timings[station].append(formatted_time)

                            else:
                                for station, (minutes, seconds) in stations.items():
                                    station_timings[station].append(timing)       

                    process_timings(station_HWMM_up, stations_up, station_timings_up)

                    process_timings(station_KESP_dn, stations_dn, station_timings_dn)

                    yield {
                        'color': 'green',
                        'line': line_color,
                        'day': day,
                        'Howrah Maidan (up)': station_HWMM_up,
                        **station_timings_up,
                        'Esplanade (dn)': station_KESP_dn,
                        **station_timings_dn,
                    }

        if line_color.lower() == 'kavi subhash - hemanta mukhopadhyay (orange line)':
            for i, day in enumerate(days_of_week):
            
                if(day == 'Weekday'): 
                    for k in range(2,4):
                        # Select the table based on its index
                        table = response.xpath(f'//table[{k}]')
                        #table = response.xpath('//table[2]')
                        rows = table.xpath('.//tr[position() >= 3]')

                        if k == 2:                     
                            #direction = 'UP'
                            station_KKSO_up = []
                            station_KSJR_up = []
                            station_KJNN_up = []
                            station_KKSK_up = []
                            station_KHMD_up = []
                            for i, row in enumerate(rows):
                                raw_station = row.xpath('.//td[1]//text()').getall()
                                station = ''.join([s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_station if s.strip()])
                                if station == 'KKSO':
                                    raw_timing = row.xpath('.//td[position() >= 3]//text()').getall()
                                    timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_timing if s.strip()]
                                    station_KKSO_up.append(timing)
                                if station == 'KSJR':
                                    # Scrape the next row (immediately after KKSO)
                                    if i + 1 < len(rows):  # Check if there is a next row
                                        next_row = rows[i + 1]
                                        raw_next_timing = next_row.xpath('.//td[position() >= 2]//text()').getall()
                                        next_timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_next_timing if s.strip()]
                                        station_KSJR_up.append(next_timing)
                                if station == 'KJNN':
                                    if i + 1 < len(rows):  # Check if there is a next row
                                        next_row = rows[i + 1]
                                        raw_next_timing = next_row.xpath('.//td[position() >= 2]//text()').getall()
                                        next_timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_next_timing if s.strip()]
                                        station_KJNN_up.append(next_timing)
                                if station == 'KKSK':
                                    if i + 1 < len(rows):  # Check if there is a next row
                                        next_row = rows[i + 1]
                                        raw_next_timing = next_row.xpath('.//td[position() >= 2]//text()').getall()
                                        next_timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_next_timing if s.strip()]
                                        station_KKSK_up.append(next_timing)            
                                if station == 'KHMD':
                                    raw_timing = row.xpath('.//td[position() >= 3]//text()').getall()
                                    timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_timing if s.strip()]
                                    station_KHMD_up.append(timing)
                            yield{
                                'color': 'orange',
                                'line': line_color,
                                'day': day,
                                'Kavi Subhash (up)': station_KKSO_up,
                                'Satyajit Ray (up)': station_KSJR_up,
                                'Jyotirindra Nandi (up)': station_KJNN_up,
                                'Kavi Sukanta (up)': station_KKSK_up,
                                'Hemanta Mukhopadhyay (up)': station_KHMD_up,      
                            }
                        if k == 3:                     
                            #direction = 'DN'
                            station_KKSO_dn = []
                            station_KSJR_dn = []
                            station_KJNN_dn = []
                            station_KKSK_dn = []
                            station_KHMD_dn = []
                            for i, row in enumerate(rows):
                                raw_station = row.xpath('.//td[1]//text()').getall()
                                station = ''.join([s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_station if s.strip()])
                                if station == 'KKSO':
                                    raw_timing = row.xpath('.//td[position() >= 3]//text()').getall()
                                    timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_timing if s.strip()]
                                    station_KKSO_dn.append(timing)
                                if station == 'KSJR':
                                    # Scrape the next row (immediately after KKSO)
                                    if i + 1 < len(rows):  # Check if there is a next row
                                        next_row = rows[i + 1]
                                        raw_next_timing = next_row.xpath('.//td[position() >= 2]//text()').getall()
                                        next_timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_next_timing if s.strip()]
                                        station_KSJR_dn.append(next_timing)
                                if station == 'KJNN':
                                    if i + 1 < len(rows):  # Check if there is a next row
                                        next_row = rows[i + 1]
                                        raw_next_timing = next_row.xpath('.//td[position() >= 2]//text()').getall()
                                        next_timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_next_timing if s.strip()]
                                        station_KJNN_dn.append(next_timing)
                                if station == 'KKSK':
                                    if i + 1 < len(rows):  # Check if there is a next row
                                        next_row = rows[i + 1]
                                        raw_next_timing = next_row.xpath('.//td[position() >= 2]//text()').getall()
                                        next_timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_next_timing if s.strip()]
                                        station_KKSK_dn.append(next_timing)            
                                if station == 'KHMD':
                                    raw_timing = row.xpath('.//td[position() >= 3]//text()').getall()
                                    timing = [s.replace('\r', '').replace('\n', '').replace('\t', '').strip() for s in raw_timing if s.strip()]
                                    station_KHMD_dn.append(timing)
                            yield{
                                'color': 'orange',
                                'line': line_color,
                                'day': day,
                                'Kavi Subhash (dn)': station_KKSO_dn,
                                'Satyajit Ray (dn)': station_KSJR_dn,
                                'Jyotirindra Nandi (dn)': station_KJNN_dn,
                                'Kavi Sukanta (dn)': station_KKSK_dn,
                                'Hemanta Mukhopadhyay (dn)': station_KHMD_dn,      
                            }   
                else:
                    pass
        
        if line_color.lower() == 'joka-esplanade (purple line)':

            for i, day in enumerate(days_of_week):
            
                if(day == 'Weekday'):                     
                        
                    station_KJKA_up = ['D','8:30:00','9:20:00','10:10:00','11:00:00','11:50:00','12:40:00','13:30:00','14:20:00','15:10:00']
                    #station_KTKP_up = ['A','8:32:40','9:22:40','10:12:40','11:02:40','11:52:40','12:42:40','13:32:40','14:22:40','15:12:40','D','8:33:00','9:23:00','10:13:00','11:03:00','11:53:00','12:43:00','13:33:00','14:23:00','15:13:00']
                    station_KTKP_up = ['D','8:33:00','9:23:00','10:13:00','11:03:00','11:53:00','12:43:00','13:33:00','14:23:00','15:13:00']
                    #station_KSKB_up = ['A','8:35:15','9:25:15','10:15:15','11:05:15','11:55:15','12:45:15','13:35:15','14:25:15','15:15:15','D','8:35:35','9:25:35','10:15:35','11:05:35','11:55:35','12:45:35','13:35:35','14:25:35','15:15:35']
                    station_KSKB_up = ['D','8:35:35','9:25:35','10:15:35','11:05:35','11:55:35','12:45:35','13:35:35','14:25:35','15:15:35']
                    #station_KBCR_up = ['A','8:38:25','9:28:25','10:18:25','11:08:25','11:58:25','12:48:25','13:38:25','14:28:25','15:18:25','D','8:38:45','9:28:45','10:18:45','11:08:45','11:58:45','12:48:45','13:38:45','14:28:45','15:18:45']
                    station_KBCR_up = ['D','8:38:45','9:28:45','10:18:45','11:08:45','11:58:45','12:48:45','13:38:45','14:28:45','15:18:45']
                    #station_KBBR_up = ['A','8:41:10','9:31:10','10:21:10','11:11:10','12:01:10','12:51:10','13:41:10','14:31:10','15:21:10','D','8:41:30','9:31:30','10:21:30','11:11:30','12:01:30','12:51:30','13:41:30','14:31:30','15:21:30']
                    station_KBBR_up = ['D','8:41:30','9:31:30','10:21:30','11:11:30','12:01:30','12:51:30','13:41:30','14:31:30','15:21:30']
                    #station_KTRT_up = ['A','8:43:25','9:33:25','10:23:25','11:13:25','12:03:25','12:53:25','13:43:25','14:33:25','15:23:25','D','8:43:45','9:33:45','10:23:45','11:13:45','12:03:45','12:53:45','13:43:45','14:33:45','15:23:45']
                    station_KTRT_up = ['D','8:43:45','9:33:45','10:23:45','11:13:45','12:03:45','12:53:45','13:43:45','14:33:45','15:23:45']
                    station_KMJH_up = ['A','8:46:30','9:36:30','10:26:30','11:16:30','12:06:30','12:56:30','13:46:30','14:36:30','15:26:30']
                                
                    station_KMJH_dn = ['D','8:55:00','9:45:00','10:35:00','11:25:00','12:15:00','13:05:00','13:55:00','14:45:00','15:35:00']
                    #station_KTRT_dn = ['A','8:57:20','9:47:20','10:37:20','11:27:20','12:17:20','13:07:20','13:57:20','14:47:20','15:37:20','D','8:57:40','9:47:40','10:37:40','11:27:40','12:17:40','13:07:40','13:57:40','14:47:40','15:37:40']
                    station_KTRT_dn = ['D','8:57:40','9:47:40','10:37:40','11:27:40','12:17:40','13:07:40','13:57:40','14:47:40','15:37:40']
                    #station_KBBR_dn = ['A','8:59:40','9:49:40','10:39:40','11:29:40','12:19:40','13:09:40','13:59:40','14:49:40','15:39:40','D','9:00:00','9:50:00','10:40:00','11:30:00','12:20:00','13:10:00','14:00:00','14:50:00','15:40:00']
                    station_KBBR_dn = ['D','9:00:00','9:50:00','10:40:00','11:30:00','12:20:00','13:10:00','14:00:00','14:50:00','15:40:00']
                    #station_KBCR_dn = ['A','9:02:30','9:52:30','10:42:30','11:32:30','12:22:30','13:12:30','14:02:30','14:52:30','15:42:30','D','9:02:50','9:52:50','10:42:50','11:32:50','12:22:50','13:12:50','14:02:50','14:52:50','15:42:50']
                    station_KBCR_dn = ['D','9:02:50','9:52:50','10:42:50','11:32:50','12:22:50','13:12:50','14:02:50','14:52:50','15:42:50']
                    #station_KSKB_dn = ['A','9:05:20','9:55:20','10:45:20','11:35:20','12:25:20','13:15:20','14:05:20','14:55:20','15:45:20','D','9:05:40','9:55:40','10:45:40','11:35:40','12:25:40','13:15:40','14:05:40','14:55:40','15:45:40']
                    station_KSKB_dn = ['D','9:05:40','9:55:40','10:45:40','11:35:40','12:25:40','13:15:40','14:05:40','14:55:40','15:45:40']
                    #station_KTKP_dn = ['A','9:07:50','9:57:50','10:47:50','11:37:50','12:27:50','13:17:50','14:07:50','14:57:50','15:47:50','D','9:08:10','9:58:10','10:48:10','11:38:10','12:28:10','13:18:10','14:08:10','14:58:10','15:48:10']
                    station_KTKP_dn = ['D','9:08:10','9:58:10','10:48:10','11:38:10','12:28:10','13:18:10','14:08:10','14:58:10','15:48:10']
                    station_KJKA_dn = ['A','9:11:00','10:01:00','10:51:00','11:41:00','12:31:00','13:21:00','14:11:00','15:01:00','15:51:00']

                    yield{
                        'color': 'purple',
                        'line': line_color, 
                        'day': day,
                        'Joka (up)': station_KJKA_up,
                        'Thakurpukur (up)': station_KTKP_up,
                        'Sakher Bazar (up)': station_KSKB_up,
                        'Behala Chowrasta (up)': station_KBCR_up,
                        'Behala Bazar (up)': station_KBBR_up,
                        'Taratala (up)': station_KTRT_up,
                        'Majherhat (up)': station_KMJH_up,
                        'Joka (dn)': station_KJKA_dn,
                        'Thakurpukur (dn)': station_KTKP_dn,
                        'Sakher Bazar (dn)': station_KSKB_dn,
                        'Behala Chowrasta (dn)': station_KBCR_dn,
                        'Behala Bazar (dn)': station_KBBR_dn,
                        'Taratala (dn)': station_KTRT_dn,
                        'Majherhat (dn)': station_KMJH_dn     
                    }   
                else:
                    pass

                    