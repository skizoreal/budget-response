import pandas as pd
import numpy as np
import plotly.express as px

# cohorts
cohorts = pd.read_csv('/Users/giorgishulaia/projects/budget-response/data/cohort-enrollment/delivery lifecycle_stages_weekly_cohort 2024-07-11T0015.csv')
def process_cohorts(cohorts=cohorts):
    to_date = [
        'Lifecycle Stages Weekly Cohort Period Week',
    ]

    to_int = [
        'Lifecycle Stages Weekly Cohort User ID'
    ]

    to_float = [
    ]

    to_str = [
        'Lifecycle Stages Weekly Cohort Cohort',
        'Admin System City City',    
    ]

    for col in to_date:
        cohorts[col] = pd.to_datetime(cohorts[col])

    for col in to_int:
        cohorts[col] = cohorts[col].astype(str).str.replace(',', '').str.replace('€', '').astype(float).astype('Int64')

    for col in to_float:
        cohorts[col] = cohorts[col].astype(str).str.replace(',', '')
        if cohorts[col].str.contains('%').any():
            cohorts[col] = cohorts[col].str.replace('%', '').astype(float) / 100
        else:
            cohorts[col] = cohorts[col].astype(float)

    for col in to_str:
        cohorts[col] = cohorts[col].astype(str)

    cohorts = cohorts.rename(
        columns={
            'Lifecycle Stages Weekly Cohort Period Week': 'period_week',
            'Admin System City City': 'city',
            'Lifecycle Stages Weekly Cohort Cohort': 'cohort',
            'Lifecycle Stages Weekly Cohort User ID': 'user_id',
        }
    )

    cohorts.to_pickle('/Users/giorgishulaia/projects/budget-response/data/cohort-enrollment/cohorts-20231218-20240708.pkl')
process_cohorts()

# orders
orders = pd.read_csv('/Users/giorgishulaia/projects/budget-response/data/orders/delivery order_order 2024-07-11T1716.csv')
def process_orders(orders=orders):
    orders = orders.drop(columns=['Unnamed: 0'])

    to_date = [
        'Created Date',
        'Created Time Time',
    ]

    to_int = [
        'ID',
        'Master User ID',
        'Vendor ID',
        'Provider ID',
        'Campaign ID',
        'Discount Level',
        'Cost Share Percentage',
    ]

    to_float = [
        'Discount Value',
        'Bolt Spend',
        'Provider Spend',
        'Total Price Before Discount Eur',
        'Total Price After Discount Eur',
        'Small Order Fee Euros',
        'Service Fee Eur',
    ]

    to_str = [
        'City',
        'Provider Name English',
        'Name',
        'Spend Objective',
        'Campaign Type',
        'Campaign discount type',
    ]

    for col in to_date:
        orders[col] = pd.to_datetime(orders[col].astype(str))

    for col in to_int:
        orders[col] = orders[col].astype(str).str.replace(',', '').str.replace('€', '').astype(float).astype('Int64')

    for col in to_float:
        orders[col] = orders[col].astype(str).str.replace(',', '').str.replace('€', '')
        if orders[col].str.contains('%').any():
            orders[col] = orders[col].str.replace('%', '').astype(float) / 100
        else:
            orders[col] = orders[col].astype(float)

    orders = orders.rename(
        columns={
            'Created Date': 'order_created_date',
            'Created Time Time': 'order_created_time',
            'City': 'city',
            'ID': 'order_id',
            'Master User ID': 'user_id',
            'Vendor ID': 'vendor_id',
            'Provider ID': 'provider_id',
            'Campaign ID': 'campaign_id',
            'Provider Name English': 'provider_name',
            'Name': 'campaign_name',
            'Spend Objective': 'spend_objective',
            'Campaign Type': 'campaign_type',
            'Discount Level': 'discount_level',
            'Cost Share Percentage': 'cost_shared_provider',
            'Campaign discount type': 'discount_type',
            'Total Price Before Discount Eur': 'price_before_discount',
            'Total Price After Discount Eur': 'price_after_discount',
            'Small Order Fee Euros': 'small_order_fee',
            'Service Fee Eur': 'service_fee',
            'Discount Value': 'discount_value',
            'Bolt Spend': 'bolt_cost',
            'Provider Spend': 'provider_cost',
        }
    )
    orders = orders.map(lambda x: np.nan if pd.isnull(x) else x)

    orders = orders[~orders['price_before_discount'].isnull()].reset_index(drop=True)
    orders = orders[~orders['provider_name'].isnull()].reset_index(drop=True)

    orders.to_pickle('/Users/giorgishulaia/projects/budget-response/data/orders/orders-20240101-20240630.pkl')
process_orders()

# ---------------------------------------------------------------------------------------------------------------
orders = pd.read_pickle('/Users/giorgishulaia/projects/budget-response/data/orders/orders-20240101-20240630.pkl')

# error: for some rows campaign_id.isnull() but ~discount_type.isnull()
# solution: examined such cases and found that there are false_discounts in the df
# note: NaNs are not included in any kind of aggregation, thus I had to make joint_id

# # print(len(orders[['order_id','campaign_id','discount_type']].drop_duplicates().reset_index(drop=True)))
# # print(len(orders[['order_id','campaign_id']].drop_duplicates().reset_index(drop=True)))
# # list(orders.columns)

# # discount_types_per_campid = orders[['order_id','campaign_id','discount_type']].drop_duplicates()
# # discount_types_per_campid['joint_id'] = discount_types_per_campid['order_id'].astype(str) + '_' + discount_types_per_campid['campaign_id'].astype(str)

# # dup_joint_ids = list(discount_types_per_campid['joint_id'].value_counts()[discount_types_per_campid['joint_id'].value_counts()>1].index)
# # dup_rows = discount_types_per_campid[discount_types_per_campid['joint_id'].isin(dup_joint_ids)].reset_index(drop=True)
# # dup_rows = pd.merge(left=dup_rows,
# #                     right=orders[['order_id','campaign_id','discount_type','discount_value','bolt_cost','provider_cost','price_before_discount','price_after_discount']],
# #                     on=['order_id','campaign_id','discount_type'],
# #                     how='left')

# checking where discount_type not null and discount_value/campaign_id is null
false_discounts = orders[
    ((~orders['discount_type'].isnull()) & (orders['discount_type']!='without')) 
    & ((orders['discount_value']==0) | (orders['discount_value'].isnull()))
][['order_id','campaign_id','discount_type','discount_value','bolt_cost','provider_cost','price_before_discount','price_after_discount']]


camp_cols = [
    'city',
    'order_id',
    'user_id',
    'campaign_id',
    'campaign_name',
    'spend_objective',
    'campaign_type',
    'discount_level',
    'cost_shared_provider',
    'discount_type',
    'price_before_discount',
    'price_after_discount',
    'discount_value',
    'bolt_cost',
    'provider_cost'
]

orders[camp_cols]

orders['total_discount'] = None
orders['total_discount_on_bolt'] = None
orders['total_discount_on_provider'] = None
orders['total_menu_discount'] = None
orders['total_menu_discount_on_bolt'] = None
orders['total_menu_discount_on_provider'] = None
orders['total_delivery_discount'] = None
orders['total_delivery_discount_on_bolt'] = None
orders['total_delivery_discount_on_provider'] = None
orders['lcs_discount'] = None
orders['lcs_discount_on_bolt'] = None
orders['lcs_discount_on_provider'] = None
orders['lcs_menu_discount'] = None
orders['lcs_menu_discount_on_bolt'] = None
orders['lcs_menu_discount_on_provider'] = None
orders['lcs_delivery_discount'] = None
orders['lcs_delivery_discount_on_bolt'] = None
orders['lcs_delivery_discount_on_provider'] = None
orders['blanket_discount'] = None
orders['blanket_discount_on_bolt'] = None
orders['blanket_discount_on_provider'] = None
orders['blanket_menu_discount'] = None
orders['blanket_menu_discount_on_bolt'] = None
orders['blanket_menu_discount_on_provider'] = None
orders['blanket_delivery_discount'] = None
orders['blanket_delivery_discount_on_bolt'] = None
orders['blanket_delivery_discount_on_provider'] = None
orders['portal_discount'] = None
orders['portal_discount_on_bolt'] = None
orders['portal_discount_on_provider'] = None
orders['portal_menu_discount'] = None
orders['portal_menu_discount_on_bolt'] = None
orders['portal_menu_discount_on_provider'] = None
orders['portal_delivery_discount'] = None
orders['portal_delivery_discount_on_bolt'] = None
orders['portal_delivery_discount_on_provider'] = None
orders['promo_discount'] = None
orders['promo_discount_on_bolt'] = None
orders['promo_discount_on_provider'] = None
orders['promo_menu_discount'] = None
orders['promo_menu_discount_on_bolt'] = None
orders['promo_menu_discount_on_provider'] = None
orders['promo_delivery_discount'] = None
orders['promo_delivery_discount_on_bolt'] = None
orders['promo_delivery_discount_on_provider'] = None

orders.info()