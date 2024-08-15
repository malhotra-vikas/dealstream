import scrapy
import dotenv
import os
import boto3

from listingDescriptionHandler import (
    generate_readable_description,
    generate_readable_title_withAI,
    generate_image_from_AI,
    resize_and_convert_image,
)

dotenv.load_dotenv()


class DealstreamDataSpider(scrapy.Spider):
    name = "dealstream_data"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_URI": "output/dealstream.json",
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    # changes the headers and cookies before new run...
    cookies = {
        "cfid": "5ceb9b4c-f18c-4007-a5cc-e8f53bf2d750",
        "cftoken": "0",
        "_gcl_au": "1.1.1293103892.1721724039",
        "__stripe_mid": "c5cc8d7a-5602-4aec-bfb8-80a78949a3aeb106cb",
        "uuid": "E6CCB29E-3663-4D8E-B138D2D4C81CF4DC",
        "hasMembership": "E6CCB29E-3663-4D8E-B138D2D4C81CF4DC",
        "_gid": "GA1.2.329061477.1723608652",
        "_gat_gtag_UA_89671999_1": "1",
        "_ga_N7W2D9NKM5": "GS1.1.1723608651.18.1.1723608670.41.0.0",
        "_ga": "GA1.2.565860145.1721724040",
        "AWSALB": "G7hHuvAkPOMmX6sw4p6/VIIaVx3Gk9Hv1AL3cTBx2VYFit2yC5hpB7bO6w0RIBaZOws4RBH6R341wGkROtGxOxH9/HtKIuuP+JHsN2Egr8cQ/y9fLnAp2n6WhphB",
        "AWSALBCORS": "G7hHuvAkPOMmX6sw4p6/VIIaVx3Gk9Hv1AL3cTBx2VYFit2yC5hpB7bO6w0RIBaZOws4RBH6R341wGkROtGxOxH9/HtKIuuP+JHsN2Egr8cQ/y9fLnAp2n6WhphB",
        "datadome": "kIDDeUKQLuWm~v_onua28NI5t2UIFzzyV9_axq_Mg6oubAMlJI6ulePgx0z2Ji3wJl43FnS4KF8Q3mWZFBLJjRPCYpA7x7aGLSzezpa4CspiJaXYxk9sINPKMlPNysTe",
    }

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        # 'cookie': 'cfid=5ceb9b4c-f18c-4007-a5cc-e8f53bf2d750; cftoken=0; _gcl_au=1.1.1293103892.1721724039; __stripe_mid=c5cc8d7a-5602-4aec-bfb8-80a78949a3aeb106cb; uuid=E6CCB29E-3663-4D8E-B138D2D4C81CF4DC; hasMembership=E6CCB29E-3663-4D8E-B138D2D4C81CF4DC; _gid=GA1.2.329061477.1723608652; _gat_gtag_UA_89671999_1=1; _ga_N7W2D9NKM5=GS1.1.1723608651.18.1.1723608670.41.0.0; _ga=GA1.2.565860145.1721724040; AWSALB=G7hHuvAkPOMmX6sw4p6/VIIaVx3Gk9Hv1AL3cTBx2VYFit2yC5hpB7bO6w0RIBaZOws4RBH6R341wGkROtGxOxH9/HtKIuuP+JHsN2Egr8cQ/y9fLnAp2n6WhphB; AWSALBCORS=G7hHuvAkPOMmX6sw4p6/VIIaVx3Gk9Hv1AL3cTBx2VYFit2yC5hpB7bO6w0RIBaZOws4RBH6R341wGkROtGxOxH9/HtKIuuP+JHsN2Egr8cQ/y9fLnAp2n6WhphB; datadome=kIDDeUKQLuWm~v_onua28NI5t2UIFzzyV9_axq_Mg6oubAMlJI6ulePgx0z2Ji3wJl43FnS4KF8Q3mWZFBLJjRPCYpA7x7aGLSzezpa4CspiJaXYxk9sINPKMlPNysTe',
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not)A;Brand";v="99.0.0.0", "Google Chrome";v="127.0.6533.100", "Chromium";v="127.0.6533.100"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    }

    def start_requests(self):
        # Get the environment variable value
        env_value = os.getenv("RUN_ENV", "local")  # Default to 'local' if not set

        if env_value == "production":

            # Path to the input file
            file_path = "/home/ubuntu/dealstream/input_urls/dealstream_url.txt"
        else:
            # Path to the input file
            file_path = (
                "/Users/vikas/builderspace/dealstream/input_urls/dealstream_url.txt"
            )

        with open(file_path, "r") as file:
            lines = file.readlines()
            # Process each line
            for line in lines:
                url = line.strip()  # Remove any leading/trailing whitespace
                if url:
                    yield scrapy.Request(
                        url,
                        callback=self.parse,
                        headers=self.headers,
                        cookies=self.cookies,
                    )

    def parse(self, response, **kwargs):
        post_cards = response.css(".post")
        for data in post_cards:
            description = data.css("p").xpath("string()").get()
            article_url = f"https://dealstream.com{data.css('h2 a::attr(href)').get()}"
            last_slash_index = article_url.rfind("/")
            ad_id = article_url[last_slash_index + 1 :]
            yield scrapy.Request(
                url=article_url,
                callback=self.parse_next,
                meta={
                    "description": description,
                    "ad_id": ad_id,
                    "article_url": article_url,
                },
                headers=self.headers,
                cookies=self.cookies,
            )

        next_page_url = response.css(
            "div.col-md-6.col-6.text-end a.btn.btn-default.btn-sm::attr(href)"
        ).get()
        if next_page_url:
            next_page = f"https://dealstream.com{next_page_url}"
            yield scrapy.Request(
                url=next_page,
                callback=self.parse,
                headers=self.headers,
                cookies=self.cookies,
            )

    def parse_next(self, response):
        title = response.css('h1[data-translatable="headline"]::text').get()
        details = response.css(".mb-2 span").xpath("string()").extract()
        listing_photo = response.css(".listing-photo::attr(src)").get()
        price_str = response.css('div.card-body p:contains("Price")::text').re_first(
            r"\$([\d,]+)"
        )
        sales_str = response.css('div.card-body p:contains("Sales")::text').re_first(
            r"\$([\d,]+)"
        )
        cash_flow_str = response.css(
            'div.card-body p:contains("Cash Flow")::text'
        ).re_first(r"\$([\d,]+)")

        # Convert the extracted strings to integers
        price = self.convert_to_int(price_str)
        sales = self.convert_to_int(sales_str)
        cash_flow = self.convert_to_int(cash_flow_str)

        ebita = cash_flow

        category = response.css(".b span:nth-child(2)::text").get()
        location = response.css(".b span:nth-child(3)::text").get()

        name = response.css("#main .mb-1 a::text").get()
        person_image = response.css(".borderless::attr(src)").get()
        person_other_info = (
            response.css(".text-info.justify-content-between")
            .xpath("string()")
            .getall()
        )
        scrapedBusinessDescription = response.meta.get("description")

        fullScrapedDescription = self.combine_description_with_details(
            scrapedBusinessDescription, details
        )

        if (fullScrapedDescription and fullScrapedDescription != 'NA' and fullScrapedDescription != ""):
            business_description = generate_readable_description(fullScrapedDescription)

            #ai_images_dict = generate_image_from_AI(business_description, article_id, businesses_title)

        else:
            business_description = fullScrapedDescription

        if (business_description and business_description != 'NA' and business_description != ""):
            title = generate_readable_title_withAI(business_description)
        else:
            title = 'NA'

        listed_by = {
            "broker-name": name if name else "",
            "broker_image": person_image if name else "",
            "broker_other_info": person_other_info if person_other_info else "",
        }

        yield {
            "ad_id": f"{response.meta.get('ad_id')}_DealStream",
            "article_url": response.meta.get("article_url"),
            "title": title,
            "source": "dealstream",
            "category": category,
            "location": location,
            "asking_price": price,
            "gross_revenue": sales,
            "cash_flow": cash_flow,
            "EBITDA": ebita,
            "listing_photo": listing_photo,
            "businessListedBy": listed_by,
            "scraped_business_description": fullScrapedDescription,
            "business_description": business_description,
            "details": details,
            "broker-phone": "",
            "broker-name": "",
        }

    @staticmethod
    def convert_to_int(value_str):
        if value_str is not None:
            return int(value_str.replace(",", ""))
        return ""

    @staticmethod
    def combine_description_with_details(self, scraped_description, details):
        parsed_details = self.parse_details(details)
        full_description = (
            scraped_description
            + "\n\n"
            + "\n".join(f"{key}: {value}" for key, value in parsed_details.items())
        )
        return full_description

    def parse_details(self, details):
        parsed_details = {}
        lines = details[0].split("\r")
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                parsed_details[key.strip()] = value.strip()
        return parsed_details
