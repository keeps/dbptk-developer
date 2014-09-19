(function() {
  'use strict';

  var dbpresServices = angular.module('dbpresServices', ['ngResource']);

  dbpresServices.factory('solrService', ['$resource',
    function($resource) {
      var port = '8983';
      var dbName = 'collection1';
      var url = 'http://localhost:' + port + '/solrService/' + dbName + '/select';
      return $resource(url, {
          wt: 'json',
          'json.wrf': 'JSON_CALLBACK',
        },
        {
          query: {method: 'JSONP'},
          search: {method: 'JSONP'},
          table: {method: 'JSONP'},
          facet: {method: 'JSONP', params: {
            q: "*:*",
            facet: true,
            start: 0,
            rows: 0
          }},
          columnMeta: {method: 'JSONP', params: {
            start: 0,
            rows: 1
          }}
        });
    }]);

  dbpresServices.factory('tableService', ['solrService',
    function(solrService) {

      var processFacetFields = function(data, metaField) {
        var facetFields = data.facet_counts.facet_fields[metaField];
        var size = facetFields.length;
        var processedResponse = [];
        for (var i = 0; i < size; i++) {
          if (i % 2 === 0) {
            if (facetFields[i + 1] > 0) {
              processedResponse.push(facetFields[i]);
            }
          }
        }
        return processedResponse;
      };

      var requestFacet = function(params, callback, args) {
        solrService.facet(params, function(data) {
          var processedFacets = processFacetFields(data, params['facet.field']);
          var newArgs = [];

          newArgs.push(processedFacets);
          if (args) {
            for (var key in args) {
              newArgs.push(args[key]);
            }
          }
          callback.apply(this, newArgs);
        });
      };

      var requestColumnMeta = function(params, prefix, callback, args) {
        solrService.columnMeta(params, function(data) {
          var newArgs = [];
          var orderedColMeta = sortRowByN(data.response.docs[0], prefix);
          newArgs.push(orderedColMeta);
          if (args) {
            for (var key in args) {
              newArgs.push(args[key]);
            }
          }
          callback.apply(this, newArgs);
        });
      };

      var requestSearch = function(params, callback, args) {
        solrService.search(params, function(data) {
          callback(data);
        });
      };

      var getSchemasMeta = function(callback) {
        var params = {
          'facet.field': metaFields.schema
        };
        requestFacet(params, callback);
      };

      var getTablesMeta = function(tableSchema, callback, args) {
        tableSchema = (typeof tableSchema !== 'undefined') ? tableSchema : '*';
        var params = {
          'facet.field': metaFields.table,
          q: metaFields.schema + ':' + tableSchema
        };
        requestFacet(params, callback, args);
      };

      var getColumnsMeta = function(columnsTableId, callback, args) {
        var params = {
          fl: metaFields.columns + '*',
          q: metaFields.tableId + ':' + columnsTableId
        };
        requestColumnMeta(params, metaFields.columns, callback, args);
      };

      var getColumnsTypeMeta = function(columnsTableId, callback, args) {
        var params = {
          fl: metaFields.columnsType + '*',
          q: metaFields.tableId + ':' + columnsTableId
        };
        requestColumnMeta(params, metaFields.columnsType, callback, args);
      };

      var getTableRows = function(tableId, from, to, sortField, sortOrder, callback) {
        var params = {
          fl: dataPrefix + '*',
          q: metaFields.tableId + ':' + tableId,
          start: from,
          rows: to,
          sort: sortField + ' ' + sortOrder
        };
        solrService.table(params, function(data) {
          var rows = data.response.docs.map(function(row) {
            return sortRowByN(row, dataPrefix);
          });
          callback(rows);
        });
      };

      var getTableNumberRows = function(tableId, callback) {
        var params = {
          q: metaFields.tableId + ':' + tableId,
        };
        solrService.table(params, function(data) {
          console.log(data);
          callback(data.response.numFound);
        });
      };

    }]);
})();