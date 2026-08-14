with source as (

    select *
    from {{ source('ecommerce_raw', 'products') }}

),

renamed as (

    select
        product_id,

        lower(trim(product_category_name)) as product_category_name,

        product_name_lenght as product_name_length,
        product_description_lenght as product_description_length,

        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,

        _ingested_at,
        _batch_id,
        _source_system,
        _source_table,
        _silver_processed_at

    from source

)

select *
from renamed
