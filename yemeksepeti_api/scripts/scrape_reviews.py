from __future__ import annotations

import logging
import argparse
import pandas as pd
from yemeksepeti_api.yemeksepeti_api import YemeksepetiApi


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

    args = parser.parse_args()
    yemeksepeti = YemeksepetiApi()
    columns = [
        "Comment",
        "CommentDate",
        "Flavour",
        "Speed",
        "Serving",
        "RestaurantDisplayName",
        "City",
    ]

    if args.cities is None:
        cities = list(map(lambda cat: cat["CatalogName"], yemeksepeti.get_catalogs()))[
            :2
        ]
    else:
        cities = args.cities

    reviews = []
    for city_count, city in enumerate(cities):
        area_ids = list(
            map(lambda area: area["Id"], yemeksepeti.get_catalog_areas(catalog=city))
        )
        for area_count, area_id in enumerate(area_ids):
            restaurants = map(
                lambda rest: rest["CategoryName"],
                yemeksepeti.search_restaurants(catalog=city, area_id=area_id),
            )
            for restaurant in restaurants:
                reviews_temp = yemeksepeti.get_restaurant_reviews(
                    category_name=restaurant, catalog=city, area_id=area_id
                )
                for rev in reviews_temp:
                    rev.update({"City": city})
                reviews.extend(reviews_temp)
                logging.info(
                    f"City ({city}): {city_count}/{len(city)} | Area: {area_count}/{len(area_ids)} | Num of reviews: {len(reviews)}"
                )

                if args.max_reviews is not None and len(reviews) >= args.max_reviews:
                    break
            if args.max_reviews is not None and len(reviews) >= args.max_reviews:
                break

            if area_count % 10 == 0:
                logging.info(
                    f"Saving the data collected until now. Num of reviews: {len(reviews)}"
                )
                pd.DataFrame(reviews)[columns].to_csv(args.output_path, index=False)

        if args.max_reviews is not None and len(reviews) >= args.max_reviews:
            break

    reviews = pd.DataFrame(reviews)[columns]
    if args.max_reviews:
        reviews = reviews[: args.max_reviews]

    reviews.to_csv(args.output_path, index=False)
