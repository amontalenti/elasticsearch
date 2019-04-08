/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.cardinality;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.IntHashSet;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.logging.log4j.Level.INFO;
import static org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus.MAX_PRECISION;
import static org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus.MIN_PRECISION;
import static org.hamcrest.Matchers.closeTo;

public class HyperLogLogPlusPlusTests extends ESTestCase {
    public void testEncodeDecode() {
        final int iters = scaledRandomIntBetween(100000, 500000);
        // random hashes
        for (int i = 0; i < iters; ++i) {
            final int p1 = randomIntBetween(4, 24);
            final long hash = randomLong();
            testEncodeDecode(p1, hash);
        }
        // special cases
        for (int p1 = MIN_PRECISION; p1 <= MAX_PRECISION; ++p1) {
            testEncodeDecode(p1, 0);
            testEncodeDecode(p1, 1);
            testEncodeDecode(p1, ~0L);
        }
    }

    private void testEncodeDecode(int p1, long hash) {
        final long index = HyperLogLogPlusPlus.index(hash, p1);
        final int runLen = HyperLogLogPlusPlus.runLen(hash, p1);
        final int encoded = HyperLogLogPlusPlus.encodeHash(hash, p1);
        assertEquals(index, HyperLogLogPlusPlus.decodeIndex(encoded, p1));
        assertEquals(runLen, HyperLogLogPlusPlus.decodeRunLen(encoded, p1));
    }

    public void testAccuracy() {
        final long bucket = randomInt(20);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 100000);
        final int p = randomIntBetween(14, MAX_PRECISION);
        IntHashSet set = new IntHashSet();
        HyperLogLogPlusPlus e = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            set.add(n);
            final long hash = BitMixer.mix64(n);
            e.collect(bucket, hash);
            if (randomInt(100) == 0) {
                //System.out.println(e.cardinality(bucket) + " <> " + set.size());
                assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
            }
        }
        assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
    }

    public void testMerge() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final HyperLogLogPlusPlus[] multi = new HyperLogLogPlusPlus[randomIntBetween(2, 100)];
        final long[] bucketOrds = new long[multi.length];
        for (int i = 0; i < multi.length; ++i) {
            bucketOrds[i] = randomInt(20);
            multi[i] = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 5);
        }
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(0, hash);
            // use a gaussian so that all instances don't collect as many hashes
            final int index = (int) (Math.pow(randomDouble(), 2));
            multi[index].collect(bucketOrds[index], hash);
            if (randomInt(100) == 0) {
                HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
                for (int j = 0; j < multi.length; ++j) {
                    merged.merge(0, multi[j], bucketOrds[j]);
                }
                assertEquals(single.cardinality(0), merged.cardinality(0));
            }
        }
    }

    public void testFakeHashes() {
        // hashes with lots of leading zeros trigger different paths in the code that we try to go through here
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);

        counts.collect(0, 0);
        assertEquals(1, counts.cardinality(0));
        if (randomBoolean()) {
            counts.collect(0, 1);
            assertEquals(2, counts.cardinality(0));
        }
        counts.upgradeToHll(0);
        // all hashes felt into the same bucket so hll would expect a count of 1
        assertEquals(1, counts.cardinality(0));
    }

    public void testPrecisionFromThreshold() {
        assertEquals(4, HyperLogLogPlusPlus.precisionFromThreshold(0));
        assertEquals(6, HyperLogLogPlusPlus.precisionFromThreshold(10));
        assertEquals(10, HyperLogLogPlusPlus.precisionFromThreshold(100));
        assertEquals(13, HyperLogLogPlusPlus.precisionFromThreshold(1000));
        assertEquals(16, HyperLogLogPlusPlus.precisionFromThreshold(10000));
        assertEquals(18, HyperLogLogPlusPlus.precisionFromThreshold(100000));
        assertEquals(18, HyperLogLogPlusPlus.precisionFromThreshold(1000000));
    }

    private HyperLogLogPlusPlus makeRandomSketch(int size) {
        // utility to make a random HLL of a given size
        HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(MAX_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 0);
        for (int i = 0; i < size; i++) {
            final int n = randomInt(100_000_000);
            final long hash = BitMixer.mix64(n);
            counts.collect(0, hash);
        }
        return counts;
    }

    public void testReadWrite() {
        // We'll test reading and writing HLL bytes for different cardinality sizes
        // ranging from 1 to 10 million. Beyond 10M, it takes awhile to generate the
        // random data.
        int[] sizes = new int[] {
            1,
            10,
            100,
            1_000,
            10_000,
            100_000,
            1_000_000, // 1 million
            10_000_000 // 10 million
        };
        for (int size : sizes) {
            // Our goal with the `baos` and `osso` is to just turn the `HyperLogLogPlusPlus.writeTo(...)`
            // call into a byte[]
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);
            HyperLogLogPlusPlus counts = makeRandomSketch(size);
            long cardinality = counts.cardinality(0);
            long precision = counts.precision();
            long maxBucket = counts.maxBucket();
            logger.printf(INFO, "%25s: [%d]", "Initial size of", size);
            logger.printf(INFO,"%25s: [%d]", "Initial cardinality of", cardinality);
            try {
                counts.writeTo(0, osso);
            } catch (IOException e) {
                fail();
            }
            // now we have the HLL as a byte[]
            byte[] hllBytes = baos.toByteArray();
            logger.printf(INFO,"%25s: [%d]", "Storing bytes on-disk", hllBytes.length);
            logger.printf(INFO,"---");
            // Our goal with `bais` and `issi` is to deserialize the byte[] into the HLL using
            // the `HyperLogLogPlusPlus.readFrom(...)` method
            ByteArrayInputStream bais = new ByteArrayInputStream(hllBytes);
            InputStreamStreamInput issi = new InputStreamStreamInput(bais);
            try {
                counts = HyperLogLogPlusPlus.readFrom(issi, BigArrays.NON_RECYCLING_INSTANCE);
            } catch (IOException e) {
                fail();
            }
            // now `counts` is our deserialized HLL; this assertion
            // confirms it by comparing their values
            assertEquals(cardinality, counts.cardinality(0));
            assertEquals(precision, counts.precision());
            assertEquals(maxBucket, counts.maxBucket());
        }
    }
}
