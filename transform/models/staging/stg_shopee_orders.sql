-- Shopee basic transformation from raw to staging
	select 
		trim(coalesce("Order ID", "Shopee - All Orders - 2021-23")) as order_id
		, trim("Order Status") as order_status
		, trim("Cancel reason") as cancel_reason
		, trim("Return / Refund Status")  as return_refund_status
		, trim("Tracking Number*") as tracking_number
		, trim("Shipping Option") as shipping_option
		, trim("Shipment Method") as shipment_method
		, cast("Estimated Ship Out Date" as timestamp) as estimated_ship_out_date
		, cast("Ship Time" as timestamp) as ship_time
		, cast("Order Creation Date" as timestamp) as order_creation_date
		, "Order Paid Time" as order_paid_date
		, trim("Parent SKU Reference No.") as parent_sku
		, trim("Product Name") as product_name
		, trim("SKU Reference No.") as gbh_sku
		, trim("Variation Name") as variation_name
		, "Original Price" as original_price
		, "Deal Price" as deal_price
		, Quantity as quantity
		, "Returned quantity" as returned_quantity
		, "Product Subtotal" as product_subtotal
		, "Seller Rebate" as seller_rebate
		, "Seller Discount" as seller_discount
		, "Shopee Rebate" as shopee_rebate
		, "SKU Total Weight" as sku_total_weight
		, "No of product in order" as num_of_product_in_order
		, "Order Total Weight" as order_total_weight
		, trim("Voucher Code") as voucher_code
		, "Seller Voucher" as seller_voucher
		, "Seller Absorbed Coin Cashback" as seller_absorbed_coin_cashback
		, "Shopee Voucher" as shopee_voucher
		, "Bundle Deal Indicator" as bundle_deal_indicator
		, "Shopee Bundle Discount" as shopee_bundle_discount
		, "Seller Bundle Discount" as seller_bundle_discount
		, "Shopee Coins Offset" as shopee_coins_offset
		, "Credit Card Discount Total" as credit_card_discount_total
		, "Total Amount" as total_amount
		, "Buyer Paid Shipping Fee" as buyer_paid_shipping_fee
		, "Shipping Rebate Estimate" as shipping_rebate_estimate
		, "Reverse Shipping Fee" as reverse_shipping_fee
		, "Transaction Fee(Incl. GST)" as transaction_fee_incl_gst
		, "Commission Fee (Incl. GST)" as commission_fee_incl_gst
		, "Service Fee (incl. GST)" as service_fee_incl_gst
		, "Grand Total" as grand_total
		, "Estimated Shipping Fee" as estimated_shipping_fee
		, trim("Username (Buyer)") as username_buyer
		, trim("Receiver Name") as receiver_name
		, trim("Phone Number") as "phone_number" 
		, trim("Delivery Address") as delivery_address
		, trim(Town) as town
		, trim(District) as district
		, trim(City) as city
		, trim(Province) as province
		, trim(Country) as country
		, "Zip Code" as zip_code
		, trim("Remark from buyer") as remark_from_buyer
		, cast("Order Complete Time" as timestamp) as order_complete_time
		, trim(Note) as note
	from {{source('raw_data','raw_shopee_orders')}}