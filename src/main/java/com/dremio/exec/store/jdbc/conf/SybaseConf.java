/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package com.dremio.exec.store.jdbc.conf;

 import static com.google.common.base.Preconditions.checkNotNull;
 import java.util.Properties;

 import javax.validation.constraints.Max;
 import javax.validation.constraints.Min;
 import javax.validation.constraints.NotBlank;

 import com.dremio.exec.catalog.conf.DisplayMetadata;
 import com.dremio.exec.catalog.conf.NotMetadataImpacting;
 import com.dremio.exec.catalog.conf.Secret;
 import com.dremio.exec.catalog.conf.SourceType;
 import com.dremio.options.OptionManager;
 import com.dremio.security.CredentialsService;
 import com.dremio.exec.store.jdbc.CloseableDataSource;
 import com.dremio.exec.store.jdbc.DataSources;
 import com.dremio.exec.store.jdbc.JdbcPluginConfig;
 import com.fasterxml.jackson.annotation.JsonIgnore;
 import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
 import com.dremio.exec.store.jdbc.dialect.SybaseDialect;
 import com.google.common.annotations.VisibleForTesting;
 import com.google.common.base.Strings;

 import io.protostuff.Tag;


/**
 * Configuration for Sybase sources.
 */
@SourceType(value = "SYBASEARP", label = "Sybase", uiConfig = "sybasearp-layout.json", externalQuerySupported = true)
public class SybaseConf extends AbstractArpConf<SybaseConf> {
  private static final String ARP_FILENAME = "arp/implementation/sybase-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (SybaseDialect::new));
  private static final String DRIVER = "com.sybase.jdbc4.jdbc.SybDriver";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Hostname")
  public String hostname;

  @NotBlank
  @Tag(2)
  @Min(1)
  @Max(65535)
  @DisplayMetadata(label = "Port")
  public String port = "5000";

  @Tag(3)
  @DisplayMetadata(label = "Database (optional)")
  public String database;

  @NotBlank
  @Tag(4)
  @DisplayMetadata(label = "Username")
  public String username;

  @NotBlank
  @Tag(5)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;

  @Tag(6)
  @DisplayMetadata(label = "Encrypt connection")
  public boolean useSsl = false;

  @Tag(7)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 500;

  @Tag(8)
  @NotMetadataImpacting
  @JsonIgnore
  @DisplayMetadata(label = "Grant External Query access (External Query allows creation of VDS from a Sybase query. Learn more here: https://docs.dremio.com/data-sources/external-queries.html#enabling-external-queries)")
  public boolean enableExternalQuery = false;

  @Tag(9)
  @DisplayMetadata(label = "Maximum idle connections")
  @NotMetadataImpacting
  public int maxIdleConns = 8;

  @Tag(10)
  @DisplayMetadata(label = "Connection idle time (s)")
  @NotMetadataImpacting
  public int idleTimeSec = 60;


  public SybaseConf() {
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(JdbcPluginConfig.Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
         return configBuilder.withDialect(getDialect())
        .withDatasourceFactory(this::newDataSource)
        .withShowOnlyConnDatabase(false)
        .withFetchSize(fetchSize)
        .withAllowExternalQuery(enableExternalQuery)
        .build();
  }


  private CloseableDataSource newDataSource() {
    final Properties properties = new Properties();

    properties.setProperty("JCONNECT_VERSION", "7");

    if (useSsl) {
      properties.setProperty("SYBSOCKET_FACTORY", "DEFAULT");
    }

  return DataSources.newGenericConnectionPoolDataSource(
    DRIVER,
    toJdbcConnectionString(),
    username,
    password,
    properties,
    DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE,
    maxIdleConns,
    idleTimeSec);
  }

  private String toJdbcConnectionString() {
    final String hostname = checkNotNull(this.hostname, "missing hostname");
    final String portAsString = checkNotNull(this.port, "missing port");
    final int port = Integer.parseInt(portAsString);

    if (!Strings.isNullOrEmpty(this.database)) {
      return String.format("jdbc:sybase:Tds:%s:%s/%s", hostname, port, this.database);
    } else {
      return String.format("jdbc:sybase:Tds:%s:%s", hostname, port);
    }
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
