from __future__ import annotations

import os
import time
import logging
import argparse
import pandas as pd
from collections import deque
from yemeksepeti_api.yemeksepeti_api import YemeksepetiApi

delay_reviews = 0.01
delay_cities = 15
delay_areas = 15
columns = [
    "Comment",
    "CommentDate",
    "Flavour",
    "Speed",
    "Serving",
    "RestaurantDisplayName",
    "City",
]

if __name__ == "__main__":

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Script for collecting Yemeksepeti reviews."
    )
    parser.add_argument(
        "-o",
        "--output_path",
        default="data.csv",
        help="Data output path (csv file). Defaults to data.csv",
    )
    parser.add_argument(
        "-c",
        "--cities",
        nargs="+",
        default=None,
        help="List of cities to get reviews from (in the TR_{CITY_NAME} format). Defaults to all cities.",
    )
    parser.add_argument(
        "-m",
        "--max_reviews",
        default=None,
        type=int,
        help="Max number of reviews to get. Defaults to unlimited.",
    )
    parser.add_argument(
        "-s",
        "--save_every_n_area",
        default=10,
        type=int,
        help="Saves collected reviews to output path every N area. Defaults to 10.",
    )

    args = parser.parse_args()
    yemeksepeti = YemeksepetiApi()

    if args.cities is None:
        cities = list(map(lambda cat: cat["CatalogName"], yemeksepeti.get_catalogs()))
    else:
        cities = args.cities

    reviews = []
    review_count = 0
    stuck_count = 0
    for city_count, city in enumerate(cities):
        time.sleep(delay_cities)
        area_ids = list(
            map(lambda area: area["Id"], yemeksepeti.get_catalog_areas(catalog=city))
        )
        for area_count, area_id in enumerate(area_ids):
            time.sleep(delay_areas)
            restaurants = map(
                lambda rest: rest["CategoryName"],
                yemeksepeti.search_restaurants(catalog=city, area_id=area_id),
            )
            for restaurant in restaurants:
                time.sleep(delay_reviews)
                reviews_temp = yemeksepeti.get_restaurant_reviews(
                    category_name=restaurant, catalog=city, area_id=area_id
                )
                deque.append(reviews_temp)

                if not reviews_temp:
                    logging.error(
                        f"Got NoneType from yemeksepeti.get_restaurant_reviews at {restaurant} {city} {area_id}"
                    )
                    if all([q is None for q in deque]):
                        stuck_count += 1
                        logging.error(
                            f"Got NoneType from yemeksepeti.get_restaurant_reviews 5 times in a row. Waiting {(600 * stuck_count)/60} minutes"
                        )
                        time.sleep(600 * stuck_count)
                        if stuck_count % 5 == 0 and stuck_count != 0:
                            delay_reviews += 0.01
                            logging.error(
                                f"Got stuck {stuck_count} times, updating delay between reviews. Delay: {delay_reviews}"
                            )

                    continue

                for rev in reviews_temp:
                    rev.update({"City": city})

                reviews.extend(reviews_temp)
                review_count += len(reviews_temp)
                logging.info(
                    f"City ({city}): {city_count}/{len(city)} | Area: {area_count}/{len(area_ids)} | Num of reviews: {review_count}"
                )

                if args.max_reviews is not None and review_count >= args.max_reviews:
                    break
            if args.max_reviews is not None and review_count >= args.max_reviews:
                break

            if area_count % args.save_every_n_area == 0:
                logging.info(
                    f"Saving the data collected until now. Num of reviews: {review_count}"
                )
                pd.DataFrame(reviews)[columns].to_csv(
                    args.output_path,
                    mode="a",
                    header=not os.path.exists(args.output_path),
                    index=False,
                )
                reviews = []

        if args.max_reviews is not None and len(reviews) >= args.max_reviews:
            break

    pd.DataFrame(reviews)[columns].to_csv(
        args.output_path,
        mode="a",
        header=not os.path.exists(args.output_path),
        index=False,
    )
