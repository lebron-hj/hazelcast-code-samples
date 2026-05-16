/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.samples.duckdb;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public final class ArrowSchemaFactory {

    public static final Schema BUYER_SCHEMA;
    public static final Schema ORDER_MAIN_SCHEMA;
    public static final Schema ORDER_ITEM_SCHEMA;

    private ArrowSchemaFactory() {
    }

    static {
        BUYER_SCHEMA = new Schema(List.of(
                Field.nullable("buyer_id", Types.MinorType.BIGINT.getType()),
                Field.nullable("buyer_nickname", Types.MinorType.VARCHAR.getType()),
                Field.nullable("buyer_real_name", Types.MinorType.VARCHAR.getType()),
                Field.nullable("buyer_phone", Types.MinorType.VARCHAR.getType()),
                Field.nullable("buyer_level", Types.MinorType.VARCHAR.getType()),
                Field.nullable("register_area", Types.MinorType.VARCHAR.getType()),
                Field.nullable("register_time", Types.MinorType.BIGINT.getType())));

        ORDER_MAIN_SCHEMA = new Schema(List.of(
                Field.nullable("order_id", Types.MinorType.BIGINT.getType()),
                Field.nullable("order_no", Types.MinorType.VARCHAR.getType()),
                Field.nullable("buyer_id", Types.MinorType.BIGINT.getType()),
                Field.nullable("create_time", Types.MinorType.BIGINT.getType()),
                Field.nullable("pay_time", Types.MinorType.BIGINT.getType()),
                Field.nullable("order_status", Types.MinorType.VARCHAR.getType()),
                Field.nullable("pay_way", Types.MinorType.VARCHAR.getType()),
                Field.nullable("order_channel", Types.MinorType.VARCHAR.getType()),
                Field.nullable("total_amount", Types.MinorType.FLOAT8.getType()),
                Field.nullable("pay_amount", Types.MinorType.FLOAT8.getType()),
                Field.nullable("freight_amount", Types.MinorType.FLOAT8.getType()),
                Field.nullable("coupon_amount", Types.MinorType.FLOAT8.getType()),
                Field.nullable("receiver_name", Types.MinorType.VARCHAR.getType()),
                Field.nullable("receiver_phone", Types.MinorType.VARCHAR.getType()),
                Field.nullable("receiver_address", Types.MinorType.VARCHAR.getType())));

        ORDER_ITEM_SCHEMA = new Schema(List.of(
                Field.nullable("item_id", Types.MinorType.BIGINT.getType()),
                Field.nullable("order_id", Types.MinorType.BIGINT.getType()),
                Field.nullable("spu_no", Types.MinorType.VARCHAR.getType()),
                Field.nullable("sku_no", Types.MinorType.VARCHAR.getType()),
                Field.nullable("goods_name", Types.MinorType.VARCHAR.getType()),
                Field.nullable("category1", Types.MinorType.VARCHAR.getType()),
                Field.nullable("category2", Types.MinorType.VARCHAR.getType()),
                Field.nullable("brand_name", Types.MinorType.VARCHAR.getType()),
                Field.nullable("original_price", Types.MinorType.FLOAT8.getType()),
                Field.nullable("sale_price", Types.MinorType.FLOAT8.getType()),
                Field.nullable("buy_num", Types.MinorType.INT.getType()),
                Field.nullable("item_subtotal", Types.MinorType.FLOAT8.getType()),
                Field.nullable("goods_spec", Types.MinorType.VARCHAR.getType())));
    }
}

