/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazon.sample.ui.services.catalog;

import com.amazon.sample.ui.client.catalog.CatalogClient;
import com.amazon.sample.ui.services.catalog.model.CatalogMapper;
import com.amazon.sample.ui.services.catalog.model.Product;
import com.amazon.sample.ui.services.catalog.model.ProductPage;
import com.amazon.sample.ui.services.catalog.model.ProductTag;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KiotaCatalogService implements CatalogService {

  private CatalogClient catalogClient;
  private CatalogMapper mapper;

  public KiotaCatalogService(
    CatalogClient catalogClient,
    CatalogMapper mapper
  ) {
    this.catalogClient = catalogClient;
    this.mapper = mapper;
  }

  @Override
  public Mono<ProductPage> getProducts(
    String tag,
    String order,
    int page,
    int size
  ) {
    var response = Mono.just(
      this.catalogClient.catalog()
        .size()
        .get(getRequestConfiguration -> {
          getRequestConfiguration.queryParameters.tags = tag;
        })
    );

    return Flux.fromIterable(
      this.catalogClient.catalog()
        .products()
        .get(getRequestConfiguration -> {
          getRequestConfiguration.queryParameters.order = order;
          getRequestConfiguration.queryParameters.page = page;
          getRequestConfiguration.queryParameters.size = size;
          getRequestConfiguration.queryParameters.tags = tag;
        })
    )
      .map(mapper::product)
      .collectList()
      .zipWith(response, (p, r) -> new ProductPage(page, size, r.getSize(), p));
  }

  @Override
  public Mono<Product> getProduct(String productId) {
    return Mono.just(
      this.catalogClient.catalog().products().byId(productId).get()
    ).map(mapper::product);
  }

  @Override
  public Flux<ProductTag> getTags() {
    return Flux.fromIterable(this.catalogClient.catalog().tags().get()).map(
      mapper::tag
    );
  }

  /**
   * The checked-in Kiota catalog client currently exposes the read operations
   * only. Keep the service contract intact and fail asynchronously until the
   * client is regenerated with POST /catalog/products support.
   */
  @Override
  public Mono<Product> createProduct(
    String token,
    String name,
    String description,
    int price
  ) {
    return Mono.error(
      new UnsupportedOperationException(
        "Catalog client does not expose POST /catalog/products"
      )
    );
  }

  /**
   * The checked-in Kiota catalog client currently exposes the read operations
   * only. Keep the service contract intact and fail asynchronously until the
   * client is regenerated with DELETE /catalog/products/{id} support.
   */
  @Override
  public Mono<Void> deleteProduct(String token, String productId) {
    return Mono.error(
      new UnsupportedOperationException(
        "Catalog client does not expose DELETE /catalog/products/{id}"
      )
    );
  }
}
