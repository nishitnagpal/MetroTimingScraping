import scrapy

class AnotherSpider(scrapy.Spider):
    name = 'another_spider'
    start_urls = ['https://mtp.indianrailways.gov.in/view_section.jsp?lang=0&id=0,2,630']

    def parse(self, response):
        lines = response.xpath('//table[@id="table18"]//tr//td[3]//a[contains(@href, "view_section.jsp")]')
        raw_line_data = {
            line.xpath('text()').get().strip(): response.urljoin(line.xpath('@href').get())
            for line in lines
        }

        # Follow each link to scrape the timetable for each line
        for color, href in raw_line_data.items():
            yield response.follow(href, callback=self.parse_timetable, meta={'line_color': color})

    def parse_timetable(self, response):
        line_color = response.meta['line_color']
        days_of_week = ['Weekday', 'Saturday', 'Sunday']

        # Determine the parsing strategy based on the line color
        if line_color.lower() == 'north-south (blue line)':
            yield from self.parse_north_south(response, days_of_week, line_color)
        elif line_color.lower() == 'east - west (green line)':
            yield from self.parse_east_west(response, days_of_week, line_color)
        elif line_color.lower() == 'kavi subhash - hemanta mukhopadhyay (orange line)':
            yield from self.parse_orange_line(response, days_of_week, line_color)

    def parse_north_south(self, response, days_of_week, line_color):
        for i, day in enumerate(days_of_week):
            table = response.xpath(f'//table[@class="MsoNormalTable"][{i+1}]')
            rows = table.xpath('.//tr[starts-with(@style, "mso-yfti-irow")][3:]')

            direction_up = self.extract_direction(response, 1, 1)
            direction_down = self.extract_direction(response, 1, 3)

            for row in rows:
                yield {
                    'line': line_color,
                    'day': day,
                    'direction_uptravel': direction_up,
                    'dep_kavi_subhash_up': self.extract_cell_text(row, 1),
                    'dep_dum_dum_up': self.extract_cell_text(row, 2),
                    'arr_dakhineshwar_up': self.extract_cell_text(row, 3),
                    'direction_dntravel': direction_down,
                    'dep_kavi_subhash_dn': self.extract_cell_text(row, 4),
                    'dep_dum_dum_dn': self.extract_cell_text(row, 5),
                    'arr_dakhineshwar_dn': self.extract_cell_text(row, 6),
                }

    def parse_east_west(self, response, days_of_week, line_color):
        for i, day in enumerate(days_of_week):
            if day == 'Saturday':
                for k in range(1, 3):
                    table = response.xpath(f'//table[@class="MsoNormalTable" and @border="1"][{k}]')
                    yield self.extract_station_data(response, table, line_color, day)
            else:
                table = response.xpath(f'//table[@class="MsoNormalTable"][{i+1}]')
                yield self.extract_station_data(response, table, line_color, day)

    def parse_orange_line(self, response, days_of_week, line_color):
        for i, day in enumerate(days_of_week):
            if day == 'Weekday':
                for k in range(2, 4):
                    table = response.xpath(f'//table[{k}]')
                    direction = 'UP' if k == 2 else 'DOWN'
                    yield self.extract_station_data(response, table, line_color, day, direction)

    def extract_direction(self, response, table_index, cell_index):
        return ''.join(
            response.xpath(f'//table[@class="MsoNormalTable"][{table_index}]//tr[contains(@style, "mso-yfti-irow:1")]//td[{cell_index}]//p[@class="MsoNormal"]/b/span/text()').getall()
        ).strip()

    def extract_cell_text(self, row, cell_index):
        return ' '.join(row.xpath(f'.//td[{cell_index}]//p[@class="MsoNormal"]/span/text()').getall()).strip()

    def extract_station_data(self, response, table, line_color, day, direction=None):
        station_names = table.xpath('.//tr[2]//td//p[@class="MsoNormal"]//b/span/text()').getall()
        rows = table.xpath('.//tr[position() >= 3]')

        station_data = {station.strip(): [] for station in station_names}

        for row in rows:
            for j, station in enumerate(station_names):
                cell_data = row.xpath(f'.//td[{j+1}]//p[@class="MsoNormal"]//span/text() | .//p[@class="MsoNormal"]//b/span/text()').get()
                if cell_data:
                    station_data[station.strip()].append(cell_data.strip())

        return {
            'line': line_color,
            'day': day,
            'direction': direction,
            **station_data
        }
