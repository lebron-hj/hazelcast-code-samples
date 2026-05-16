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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class EcommerceMockGenerator {

    private static final Random RANDOM = new Random(20260515L);
    private static final AtomicLong BUYER_ID_GEN = new AtomicLong(10000L);
    private static final AtomicLong ORDER_ID_GEN = new AtomicLong(1_000_000L);
    private static final AtomicLong ITEM_ID_GEN = new AtomicLong(10_000_000L);

    private static final List<String> BUYER_LEVEL_LIST = List.of("普通会员", "白银会员", "黄金会员", "钻石会员");
    private static final List<String> PAY_WAY_LIST = List.of("支付宝", "微信支付", "银行卡", "抖音分期", "花呗");
    private static final List<String> ORDER_STATUS_LIST = List.of("待付款", "已付款", "已发货", "已完成", "已取消", "已退款");
    private static final List<String> ORDER_CHANNEL_LIST = List.of("APP商城", "微信小程序", "H5网页", "直播带货", "线下门店");
    private static final List<String> CATEGORY1_LIST = List.of("数码家电", "服饰鞋包", "食品生鲜", "家居日用", "美妆护肤");
    private static final List<String> BRAND_LIST = List.of("华为", "小米", "苹果", "耐克", "李宁", "三只松鼠", "欧莱雅");
    private static final List<String> AREA_LIST = List.of("广东省深圳市", "浙江省杭州市", "上海市", "北京市", "四川省成都市");

    private EcommerceMockGenerator() {
    }

    public static List<EcommerceOrderBatch> generateBatches(int count) {
        List<EcommerceOrderBatch> batches = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batches.add(generateOrderBatch(i + 1L));
        }
        return batches;
    }

    public static EcommerceOrderBatch generateOrderBatch(long batchId) {
        EcommerceBuyer buyer = mockBuyerInfo();
        EcommerceOrder order = mockOrderMain(buyer.buyerId(), buyer.buyerRealName());
        List<EcommerceOrderItem> items = new ArrayList<>();
        int itemCount = 2 + RANDOM.nextInt(4);
        for (int i = 0; i < itemCount; i++) {
            String category1 = CATEGORY1_LIST.get(RANDOM.nextInt(CATEGORY1_LIST.size()));
            items.add(mockOrderItem(order.orderId(), category1));
        }
        return new EcommerceOrderBatch(batchId, buyer, order, items, List.of());
    }

    public static EcommerceOrderBatch generateDeleteBatch(long batchId, long orderId) {
        return EcommerceOrderBatch.deleteOnly(batchId, orderId);
    }

    private static EcommerceBuyer mockBuyerInfo() {
        long buyerId = BUYER_ID_GEN.incrementAndGet();
        String token = UUID.randomUUID().toString().substring(0, 6);
        return new EcommerceBuyer(
                buyerId,
                "buyer-" + token,
                "买家" + buyerId,
                "1" + String.format("%010d", RANDOM.nextInt(1_000_000_000)),
                BUYER_LEVEL_LIST.get(RANDOM.nextInt(BUYER_LEVEL_LIST.size())),
                AREA_LIST.get(RANDOM.nextInt(AREA_LIST.size())),
                System.currentTimeMillis() - RANDOM.nextLong(31_536_000_000L));
    }

    private static EcommerceOrder mockOrderMain(long buyerId, String buyerRealName) {
        long now = System.currentTimeMillis();
        double freightAmount = RANDOM.nextInt(50);
        double couponAmount = RANDOM.nextBoolean() ? 0.0 : 8.0;
        double totalAmount = 50.0 + RANDOM.nextDouble() * 1000.0;
        double payAmount = totalAmount - couponAmount + freightAmount;
        double payTime = now + RANDOM.nextInt(3_600_000);
        return new EcommerceOrder(
                ORDER_ID_GEN.incrementAndGet(),
                "ORD" + now + RANDOM.nextInt(1_000),
                buyerId,
                now,
                (long) payTime,
                ORDER_STATUS_LIST.get(RANDOM.nextInt(ORDER_STATUS_LIST.size())),
                PAY_WAY_LIST.get(RANDOM.nextInt(PAY_WAY_LIST.size())),
                ORDER_CHANNEL_LIST.get(RANDOM.nextInt(ORDER_CHANNEL_LIST.size())),
                totalAmount,
                payAmount,
                freightAmount,
                couponAmount,
                buyerRealName + "收",
                "1" + String.format("%010d", RANDOM.nextInt(1_000_000_000)),
                AREA_LIST.get(RANDOM.nextInt(AREA_LIST.size())) + " " + (1 + RANDOM.nextInt(20)) + "号");
    }

    private static EcommerceOrderItem mockOrderItem(long orderId, String category1) {
        double originalPrice = 100.0 + RANDOM.nextDouble() * 500.0;
        double salePrice = originalPrice * (0.6 + RANDOM.nextDouble() * 0.3);
        int buyNum = 1 + RANDOM.nextInt(4);
        String brandName = BRAND_LIST.get(RANDOM.nextInt(BRAND_LIST.size()));
        return new EcommerceOrderItem(
                ITEM_ID_GEN.incrementAndGet(),
                orderId,
                "SPU" + String.format("%06d", RANDOM.nextInt(999_999)),
                "SKU" + String.format("%08d", RANDOM.nextInt(99_999_999)),
                category1 + "商品",
                category1,
                category1 + "二级",
                brandName,
                originalPrice,
                salePrice,
                buyNum,
                salePrice * buyNum,
                RANDOM.nextBoolean() ? "标准版" : "高配版");
    }
}

