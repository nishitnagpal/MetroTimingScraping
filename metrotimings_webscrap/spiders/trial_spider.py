import scrapy
from datetime import datetime, timedelta

class TrialSpider(scrapy.Spider):
    name = 'trial_spider'
    start_urls = ['https://mtp.indianrailways.gov.in/view_section.jsp?lang=0&id=0,2,630,658']

    def parse(self, response):

        days_of_week = ['Weekday', 'Saturday', 'Sunday']

        for i, day in enumerate(days_of_week):
            
            if (day == 'Weekday' or day == 'Saturday'):
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

                yield{
                    'color': 'green',
                    #'line': line_color,
                    'day': day,
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
                    #'line': line_color,
                    'day': day,
                    'Howrah Maidan (up)': station_HWMM_up,
                    **station_timings_up,
                    'Esplanade (dn)': station_KESP_dn,
                    **station_timings_dn,
                }
                
            
   