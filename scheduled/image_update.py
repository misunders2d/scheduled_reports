import asyncio
import os
import sys
import time
from typing import List, Literal

from base.authentication import get_listings_class
from dotenv import load_dotenv
from sp_utils import send_telegram_message

load_dotenv()
MARKETPLACE_IDS = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2"]
SELLER_ID = os.environ["SELLER_ID"]

credentials = dict(
    refresh_token=os.environ["REFRESH_TOKEN_US"],
    lwa_app_id=os.environ["CLIENT_ID"],
    lwa_client_secret=os.environ["CLIENT_SECRET"],
)


image_attribute_paths = Literal[
    "main_product_image_locator",
    "other_product_image_locator_1",
    "other_product_image_locator_2",
    "other_product_image_locator_3",
    "other_product_image_locator_4",
    "other_product_image_locator_5",
    "other_product_image_locator_6",
    "other_product_image_locator_7",
    "other_product_image_locator_8",
    "swatch_product_image_locator",
]

product_details = [
    {
        "skus": [
            "BedSheetSet-King-Light-Gray-FBA",
            "M-BEDSHEETSET-K-LIGHT-GRAY-PAK",
            "M-BEDSHEETSET-K-LIGHT-GRAY-CMB",
        ],
        "MORNING_IMAGE": "https://ik.imagekit.io/jgp5dmcfb/Day-night/morning.png",
        "EVENING_IMAGE": "https://ik.imagekit.io/jgp5dmcfb/Day-night/evening.png",
        "STANDARD_IMAGE": "https://ik.imagekit.io/jgp5dmcfb/New_Iconic_Sheets_Set/1._Iconic_Sheet_Set_4pc_Light_Gray_Stack_2.jpg",
    },
    {
        "skus": [
            "BedSheetSet-Full-Light-Gray-FBA",
            "M-BEDSHEETSET-F-LIGHT-GRAY-CMB",
            "M-BEDSHEETSET-F-LIGHT-GRAY-PAK",
            "M-BED-SHEET-SET-F-LIGHT-GRAY-CA",
        ],
        "MORNING_IMAGE": "https://ik.imagekit.io/jgp5dmcfb/Day-night/4pc_Light_Gray_Daytime1.png",
        "EVENING_IMAGE": "https://ik.imagekit.io/jgp5dmcfb/Day-night/4pc_Light_Gray_Night1.jpeg",
        "STANDARD_IMAGE": "https://ik.imagekit.io/jgp5dmcfb/New_Iconic_Sheets_Set/1._Iconic_Sheet_Set_4pc_Light_Gray_Stack_2.jpg",
    },
]


async def get_listing_details(
    sku: str,
    include: List[
        Literal[
            "summaries",
            "attributes",
            "issues",
            "offers",
            "fulfillmentAvailability",
            "procurement",
            "relationships",
            "productTypes",
        ]
    ],
):

    async with get_listings_class() as client:
        response = await client.get_listings_item(
            sellerId=SELLER_ID, sku=sku, includedData=include
        )
    return response


async def update_image(
    sku,
    product_type,
    image_path,
    op: Literal["replace", "delete"] = "replace",
    attribute_path: Literal[image_attribute_paths] = "main_product_image_locator",
):

    patch_body = {
        "productType": product_type,
        "patches": [
            {
                "op": op,
                "path": f"/attributes/{attribute_path}",  # other_product_image_locator_8
                "value": [{"media_location": image_path}],
            }
        ],
    }
    try:
        async with get_listings_class() as client:
            response = await client.patch_listings_item(
                sellerId=SELLER_ID,
                sku=sku,
                marketplaceIds=MARKETPLACE_IDS,
                body=patch_body,
            )
            await send_telegram_message(
                f"Image updated for {sku} with status {response.payload['status']}\nImage: {image_path}\n\n"
            )
    except Exception as e:
        await send_telegram_message(f"FAILED to update image for {sku}:\n{e}")
        return e


def batch_delete_image(
    SKUS,
    product_type,
    image_path,
    op: Literal["replace", "delete"] = "delete",
    attribute_path: Literal[image_attribute_paths] = "other_product_image_locator_8",
):
    failed_images = {}
    for sku in SKUS:
        result = update_image(
            sku, product_type, image_path, op=op, attribute_path=attribute_path
        )
        time.sleep(1 / 5)
        if result:
            failed_images[sku] = result
    return failed_images


async def main():
    args = sys.argv[1:]
    await send_telegram_message(f"Starting cron job with argument {sys.argv[1:]}")

    for product in product_details:
        SKUS = product["skus"]
        image = product["STANDARD_IMAGE"]
        if len(args) > 0 and args[0] == "1":
            image = product["MORNING_IMAGE"]
        elif len(args) > 0 and args[0] == "2":
            image = product["EVENING_IMAGE"]
        product_type_job = await get_listing_details(
            sku=SKUS[0], include=["summaries", "productTypes"]
        )
        product_type = product_type_job.payload["summaries"][0]["productType"]
        for sku in SKUS:
            await update_image(sku, product_type=product_type, image_path=image)
            time.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
