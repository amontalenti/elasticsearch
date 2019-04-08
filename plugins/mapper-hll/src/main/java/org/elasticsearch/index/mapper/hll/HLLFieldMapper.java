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

package org.elasticsearch.index.mapper.hll;

import com.carrotsearch.hppc.BitMixer;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HLLFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "hll";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new Murmur3FieldType();
        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, HLLFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public HLLFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new HLLFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType.setIndexOptions(IndexOptions.NONE);
            defaultFieldType.setIndexOptions(IndexOptions.NONE);
            fieldType.setHasDocValues(true);
            defaultFieldType.setHasDocValues(true);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node,
                ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);

            // tweaking these settings is no longer allowed, the entire purpose of murmur3 fields is to store a hash
            if (node.get("doc_values") != null) {
                throw new MapperParsingException("Setting [doc_values] cannot be modified for field [" + name + "]");
            }
            if (node.get("index") != null) {
                throw new MapperParsingException("Setting [index] cannot be modified for field [" + name + "]");
            }

            if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha2)) {
                node.remove("precision_step");
            }

            TypeParsers.parseField(builder, name, node, parserContext);

            return builder;
        }
    }

    // this only exists so a check can be done to match the field type to using murmur3 hashing...
    public static class Murmur3FieldType extends MappedFieldType {
        public Murmur3FieldType() {
        }

        protected Murmur3FieldType(Murmur3FieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Murmur3FieldType clone() {
            return new Murmur3FieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new BytesBinaryDVIndexFieldData.Builder();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "HLL fields are not searchable: [" + name() + "]");
        }
    }

    protected HLLFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields)
            throws IOException {
        // TODO: how do we take multiple values and HLL them?
        // Ideas:
        // - do we need to use parse() rather than parseCreateField()?
        // - should we copy GeoPointFieldMapper, since that impl uses parse() and START_ARRAY and similar?

        final Object value;

        // try to parse it as a map
        XContentParser.Token token = context.parser().nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            Map<String, Object> map = context.parser().map();
            assert map.containsKey("items") : "HLL object does not contain 'items' key";
            assert map.containsKey("precision") : "HLL object does not contain 'precision' key";
            Object itemsObject = map.get("items");
            List<String> itemList = (List)itemsObject;
            value = itemList.get(0);
        } else {
            // controlling the context is what allows us to access array and other values
            if (context.externalValueSet()) {
                value = context.externalValue();
            } else {
                value = context.parser().textOrNull();
            }
        }

        if (value != null) {
            // value is turned into bytes
            final BytesRef bytes = new BytesRef(value.toString());
            // those bytes are hashed
            final long hash = MurmurHash3.hash128(bytes.bytes,
                bytes.offset, bytes.length, 0, new MurmurHash3.Hash128()).h1;

            // all the above is ignored and a hard-coded byte[] is added to the index, with the HLL format
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);

            // TODO: rollupPrecision should be part of indexing op
            final int rollupPrecision = 18;
            HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(rollupPrecision, BigArrays.NON_RECYCLING_INSTANCE, 0);

            final long mixedHash = BitMixer.mix64(hash);

            // hashed bytes offered toHLL
            counts.collect(0, mixedHash);

            // TODO: below are unused, for debugging only
            long cardinality = counts.cardinality(0);
            long precision = counts.precision();
            long maxBucket = counts.maxBucket();
            try {
                // FIXME: is it right to always specify bucket as 0 here?
                counts.writeTo(0, osso);
            } catch (IOException e) {
                // FIXME: when does this IOException actually happen?
            } finally {
                osso.close();
            }
            // HLL itself converted into a byte[]
            byte[] hllBytes = baos.toByteArray();
            int hllBytesLength = hllBytes.length;
            assert hllBytesLength != 0 : "Encoded HLL had no bytes";
            // TODO: below assumption will break once we support multi-value here
            assert hllBytesLength == 7 : "Encoded HLL did not have 7 bytes";
            // FIXME: is it OK to use same BytesRef instance across two ops below?
            BytesRef hllBytesRef = new BytesRef(hllBytes);

            // stored as binary DocValues field
            //fields.add(new BinaryDocValuesField(fieldType().name(), hllBytesRef));
            fields.add(new SortedDocValuesField(fieldType().name(), hllBytesRef));

            // also stored in field storage (optionally, for reindexing)
            if (fieldType().stored()) {
                // TODO: this field mapping will need to somehow support reindexing from byte[] storage?
                fields.add(new StoredField(name(), hllBytesRef));
            }
        }
    }

}
