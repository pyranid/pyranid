/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import org.openjdk.jmh.annotations.*;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Representative mapping shapes. Run with {@code -prof gc} for allocations and at least three forks.
 * Cold includes constructing a mapper and its first plan; churn rotates 128 schemas through a 32-plan cache.
 * Synthetic JDBC isolates mapper costs; real driver integration remains a separate correctness gate.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class MappingShapeBenchmark {
	@State(Scope.Thread)
	public static class MappingState {
		@Param({"scalar-warm", "text-warm", "record1-warm", "bean1-warm", "record6-warm", "bean6-warm",
				"record32-warm", "bean32-warm", "record6-cold", "bean6-cold", "record6-uncached",
				"bean6-uncached", "record6-churn", "custom6-warm", "collection-warm"})
		public String scenario;
		ResultSetMapper mapper;
		StatementContext<?> context;
		Class<?> target;
		ResultSet[] rows;
		InstanceProvider instances = new InstanceProvider() {};
		int cursor;
		boolean cold;

		@Setup(Level.Trial)
		public void setup() throws Exception {
			String shape = scenario.substring(0, scenario.indexOf('-'));
			int width = shape.endsWith("32") ? 32 : shape.endsWith("6") ? 6 : 1;
			this.target = switch (shape) {
				case "scalar" -> Integer.class;
				case "text" -> String.class;
				case "record1" -> Record1.class;
				case "bean1" -> Bean1.class;
				case "record6", "custom6" -> Record6.class;
				case "bean6" -> Bean6.class;
				case "record32" -> Record32.class;
				case "bean32" -> Bean32.class;
				case "collection" -> CollectionRow.class;
				default -> throw new IllegalArgumentException(shape);
			};
			this.cold = scenario.endsWith("-cold");
			ResultSetMapper.Builder builder = ResultSetMapper.withPlanCachingEnabled(!scenario.endsWith("-uncached")).planCacheCapacity(32);
			if (shape.equals("custom6")) builder.customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == Integer.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
						TargetType target, Integer index, String label, InstanceProvider provider) {
					return MappingResult.fallback();
				}
			}));
			this.mapper = builder.build();
			Database database = Database.withDataSource(BenchmarkSupport.throwingDataSource()).databaseType(DatabaseType.GENERIC).build();
			this.context = context(database, this.target);
			this.rows = new ResultSet[scenario.endsWith("-churn") ? 128 : 1];
			for (int schema = 0; schema < rows.length; schema++) {
				String[] labels = new String[width];
				int[] types = new int[width];
				String[] names = new String[width];
				Object[] values = new Object[width];
				for (int column = 0; column < width; column++) {
					labels[column] = "c" + column;
					types[column] = Types.INTEGER;
					names[column] = "INTEGER_" + schema;
					values[column] = column + 1;
				}
				if (shape.equals("text")) { types[0] = Types.VARCHAR; names[0] = "VARCHAR"; values[0] = "hello"; }
				if (shape.equals("collection")) { types[0] = Types.ARRAY; names[0] = "INTEGER ARRAY"; values[0] = new Integer[]{1, 2, 3, 4, 5, 6}; }
				rows[schema] = BenchmarkSupport.resultSet(labels, types, names, values);
			}
			map(this.mapper, this.context, rows[0], this.target, this.instances);
		}
	}

	@Benchmark
	public Object mapRow(MappingState state) throws Exception {
		ResultSetMapper mapper = state.cold ? ResultSetMapper.withDefaultConfiguration() : state.mapper;
		return map(mapper, state.context, state.rows[state.cursor++ & (state.rows.length - 1)], state.target, state.instances);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Optional<?> map(ResultSetMapper mapper, StatementContext context, ResultSet row, Class target, InstanceProvider instances) throws Exception {
		return mapper.map(context, row, target, instances);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static StatementContext<?> context(Database database, Class target) {
		return StatementContext.with(Statement.of("mapping-shape", "SELECT columns"), database).resultSetRowType(target).build();
	}

	public record CollectionRow(List<Long> c0) {}
	public record Record1(Integer c0) {}
	public static class Bean1 {
		private Integer c0;
		public Integer getC0() { return this.c0; }
		public void setC0(Integer value) { this.c0 = value; }
	}
	public record Record6(Integer c0, Integer c1, Integer c2, Integer c3, Integer c4, Integer c5) {}
	public static class Bean6 {
		private Integer c0;
		public Integer getC0() { return this.c0; }
		public void setC0(Integer value) { this.c0 = value; }
		private Integer c1;
		public Integer getC1() { return this.c1; }
		public void setC1(Integer value) { this.c1 = value; }
		private Integer c2;
		public Integer getC2() { return this.c2; }
		public void setC2(Integer value) { this.c2 = value; }
		private Integer c3;
		public Integer getC3() { return this.c3; }
		public void setC3(Integer value) { this.c3 = value; }
		private Integer c4;
		public Integer getC4() { return this.c4; }
		public void setC4(Integer value) { this.c4 = value; }
		private Integer c5;
		public Integer getC5() { return this.c5; }
		public void setC5(Integer value) { this.c5 = value; }
	}
	public record Record32(Integer c0, Integer c1, Integer c2, Integer c3, Integer c4, Integer c5, Integer c6, Integer c7, Integer c8, Integer c9, Integer c10, Integer c11, Integer c12, Integer c13, Integer c14, Integer c15, Integer c16, Integer c17, Integer c18, Integer c19, Integer c20, Integer c21, Integer c22, Integer c23, Integer c24, Integer c25, Integer c26, Integer c27, Integer c28, Integer c29, Integer c30, Integer c31) {}
	public static class Bean32 {
		private Integer c0;
		public Integer getC0() { return this.c0; }
		public void setC0(Integer value) { this.c0 = value; }
		private Integer c1;
		public Integer getC1() { return this.c1; }
		public void setC1(Integer value) { this.c1 = value; }
		private Integer c2;
		public Integer getC2() { return this.c2; }
		public void setC2(Integer value) { this.c2 = value; }
		private Integer c3;
		public Integer getC3() { return this.c3; }
		public void setC3(Integer value) { this.c3 = value; }
		private Integer c4;
		public Integer getC4() { return this.c4; }
		public void setC4(Integer value) { this.c4 = value; }
		private Integer c5;
		public Integer getC5() { return this.c5; }
		public void setC5(Integer value) { this.c5 = value; }
		private Integer c6;
		public Integer getC6() { return this.c6; }
		public void setC6(Integer value) { this.c6 = value; }
		private Integer c7;
		public Integer getC7() { return this.c7; }
		public void setC7(Integer value) { this.c7 = value; }
		private Integer c8;
		public Integer getC8() { return this.c8; }
		public void setC8(Integer value) { this.c8 = value; }
		private Integer c9;
		public Integer getC9() { return this.c9; }
		public void setC9(Integer value) { this.c9 = value; }
		private Integer c10;
		public Integer getC10() { return this.c10; }
		public void setC10(Integer value) { this.c10 = value; }
		private Integer c11;
		public Integer getC11() { return this.c11; }
		public void setC11(Integer value) { this.c11 = value; }
		private Integer c12;
		public Integer getC12() { return this.c12; }
		public void setC12(Integer value) { this.c12 = value; }
		private Integer c13;
		public Integer getC13() { return this.c13; }
		public void setC13(Integer value) { this.c13 = value; }
		private Integer c14;
		public Integer getC14() { return this.c14; }
		public void setC14(Integer value) { this.c14 = value; }
		private Integer c15;
		public Integer getC15() { return this.c15; }
		public void setC15(Integer value) { this.c15 = value; }
		private Integer c16;
		public Integer getC16() { return this.c16; }
		public void setC16(Integer value) { this.c16 = value; }
		private Integer c17;
		public Integer getC17() { return this.c17; }
		public void setC17(Integer value) { this.c17 = value; }
		private Integer c18;
		public Integer getC18() { return this.c18; }
		public void setC18(Integer value) { this.c18 = value; }
		private Integer c19;
		public Integer getC19() { return this.c19; }
		public void setC19(Integer value) { this.c19 = value; }
		private Integer c20;
		public Integer getC20() { return this.c20; }
		public void setC20(Integer value) { this.c20 = value; }
		private Integer c21;
		public Integer getC21() { return this.c21; }
		public void setC21(Integer value) { this.c21 = value; }
		private Integer c22;
		public Integer getC22() { return this.c22; }
		public void setC22(Integer value) { this.c22 = value; }
		private Integer c23;
		public Integer getC23() { return this.c23; }
		public void setC23(Integer value) { this.c23 = value; }
		private Integer c24;
		public Integer getC24() { return this.c24; }
		public void setC24(Integer value) { this.c24 = value; }
		private Integer c25;
		public Integer getC25() { return this.c25; }
		public void setC25(Integer value) { this.c25 = value; }
		private Integer c26;
		public Integer getC26() { return this.c26; }
		public void setC26(Integer value) { this.c26 = value; }
		private Integer c27;
		public Integer getC27() { return this.c27; }
		public void setC27(Integer value) { this.c27 = value; }
		private Integer c28;
		public Integer getC28() { return this.c28; }
		public void setC28(Integer value) { this.c28 = value; }
		private Integer c29;
		public Integer getC29() { return this.c29; }
		public void setC29(Integer value) { this.c29 = value; }
		private Integer c30;
		public Integer getC30() { return this.c30; }
		public void setC30(Integer value) { this.c30 = value; }
		private Integer c31;
		public Integer getC31() { return this.c31; }
		public void setC31(Integer value) { this.c31 = value; }
	}
}
