def send_new_products(new_products):
    # Previous payload structure
    payload = {
        "products": new_products,  # changed from "data" to "products"
    }
    # send the payload to the desired endpoint
    send_payload_to_endpoint(payload)


def test_webhook(webhook_type):
    if webhook_type == "new":
        # Previous payload structure
        payload = {
            "products": some_value,  # changed from "data" to "products"
        }
        # process the payload
        process_payload(payload)
