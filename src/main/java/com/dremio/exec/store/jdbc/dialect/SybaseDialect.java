/*
 * Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectOperator;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.common.expression.CompleteType;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * A <code>SqlDialect</code> implementation for the Sybase database.
 */
public class SybaseDialect extends ArpDialect {

  /** Creates a SybaseSqlDialect. */
  public SybaseDialect(ArpYaml yaml) {
    super(yaml);
  }

  @Override
  public boolean hasImplicitTableAlias() {
    return false;
  }

  @Override
  public boolean supportsLiteral(CompleteType type) {
    if (CompleteType.BIT.equals(type)) {
      return false;
    }

    return super.supportsLiteral(type);
  }

  @Override
  public void unparseDateTimeLiteral(SqlWriter writer,
    SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    writer.literal("'" + literal.toFormattedString() + "'");
  }

  @Override
  public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
    // Transform SqlSelect nodes that have a fetch node to be SqlSelect nodes with a TOP and no fetch.
    if (call instanceof SqlSelect) {
      final SqlSelect select = (SqlSelect) call;

      // Transform SqlSelect nodes that have a fetch node without offset to be
      // SqlSelect nodes with a TOP and no fetch.
      if (null != select.getFetch()
        && (null == select.getOffset() || 0 == ((SqlLiteral) select.getOffset()).getValueAs(Long.class))) {
        final SqlNodeList keywords = new SqlNodeList(SqlParserPos.ZERO);

        // Add the DISTINCT or ALL keywords if the original Select had either. (Only can have one of these).
        // These must go before TOP.
        if (null != select.getModifierNode(SqlSelectKeyword.DISTINCT)) {
          keywords.add(select.getModifierNode(SqlSelectKeyword.DISTINCT));
        } else if (null != select.getModifierNode(SqlSelectKeyword.ALL)) {
          keywords.add(select.getModifierNode(SqlSelectKeyword.ALL));
        }

        // Inject the TOP <literal> nodes.
        keywords.add(SqlSelectExtraKeyword.TOP.symbol(SqlParserPos.ZERO));
        keywords.add(select.getFetch());

        // Unparse a version of this select with TOP injected and the FETCH removed.
        final SqlSelect modifiedSelect = SqlSelectOperator.INSTANCE.createCall(
          keywords,
          select.getSelectList(),
          select.getFrom(),
          select.getWhere(),
          select.getGroup(),
          select.getHaving(),
          select.getWindowList(),
          select.getOrderList(),
          null,
          null,
          SqlParserPos.ZERO);

        super.unparseCall(writer, modifiedSelect, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    } else {
      // Fall through to the base class implementation.
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }
}

// End SybaseDialect.java
