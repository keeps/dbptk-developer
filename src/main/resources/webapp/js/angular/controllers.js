(function() {
  'use strict';

  var dbpresControllers = angular.module('dbpresControllers', ['ui.bootstrap']);

  dbpresControllers.controller('TableCtrl', ['$scope', '$http', 'solrService', 'tableService',
    function($scope, $http, solrService, tableService) {

      $scope.parseInt = parseInt;

      $scope.tableId = 'id';
      $scope.schemas = {};
      $scope.rowNumbers = [10, 25, 50, 100];

      var metaPrefix = 'dbpres_meta_';
      var dataPrefix = 'dbpres_data_';
      var metaFields = {
        id: metaPrefix + 'id',
        tableId: metaPrefix + 'tableId',
        table: metaPrefix + 'table',
        schema: metaPrefix + 'schema',
        rowN: metaPrefix + 'rowN',
        columns: metaPrefix + 'col_',
        columnsType: metaPrefix + 'colType_'
        // complete..
      };

      var handleColumns = function(columns, schemaName, tableName) {
        var tables = $scope.schemas[schemaName].tables;
        tables[tableName].columns = columns;
      };

      var handleColumnsType = function(columnsType, schemaName, tableName) {
        var tables = $scope.schemas[schemaName].tables;
        tables[tableName].columnsType = columnsType;
      };

      var handleTables = function(tables, schemaName) {
        var schemas = $scope.schemas;
        schemas[schemaName].tables = {};
        for (var key in tables) {
          var tableName = tables[key];
          var tableId = schemaName + "." + tableName;
          var table = schemas[schemaName].tables[tableName] = {};
          table.id = tableId;
          // getColumnsMeta(tableId, handleColumns, [schemaName, tableName]);
          // getColumnsTypeMeta(tableId, handleColumnsType, [schemaName, tableName]);
        }
        
      };

      tableService.getSchemasMeta(function(schemasNames) {
        for (var key in schemasNames) {
          var schemaName = schemasNames[key];
          $scope.schemas[schemaName] = {};
          getTablesMeta(schemaName, handleTables, [schemaName]);
        }
        getRandomTableId($scope.schemas);
        $scope.$watch('tableId', function(o, n) {
          if (o != n) {
            $scope.showTable($scope.tableId);
          }
        }, true);
      });

      $scope.showTable = function(tableId) {
        $scope.start = 0;
        $scope.rows = 10;
        $scope.sortASC = true;
        $scope.orderByField = 'col-1';
        $scope.currentCol = 'col-1';
        $scope.tableId = tableId;
        
        getColumnsMeta(tableId, function(columns) {
          $scope.tableCols = columns;
        });

        getColumnsTypeMeta(tableId, function(columnsType) {
          $scope.tableColsType = columnsType;
        });

        getTableRows(tableId, $scope.start, $scope.rows, metaFields.rowN, "ASC", function(rows) {
          $scope.tableRows = rows;
        });

        getTableNumberRows(tableId, function(nFound) {
          $scope.numFound = nFound;
        });
      };

      $scope.sortRows = function(tableId, start, offSet, colN) {
        if ($scope.currentCol !== $scope.orderByField) {
          $scope.currentCol = $scope.orderByField;
        } else {
          $scope.sortASC = !$scope.sortASC;
        }

        if ($scope.sortASC) {
          $scope.order = "ASC";
        } else {
          $scope.order = "DESC";
        }

        getTableRows(tableId, $scope.start, $scope.rows, dataPrefix + colN, $scope.order, function(rows) {
          $scope.tableRows = rows;
        });
      };


      $scope.searchTable = function(tableId) {
        console.log($scope.querySearch);
        var defaultQuery = metaFields.tableId + ':' + tableId;
        
        if ($scope.querySearch !== "") {
          defaultQuery += ' AND ' + '*' + $scope.querySearch + '*';
        }
        var params = {
          q: defaultQuery,
          fl: dataPrefix + '*',
          sort: metaFields.rowN + " asc"
        };

        requestSearch(params, function(data) {
          var docs = data.response.docs;
          // console.log(docs);
          $scope.tableRows = docs;
        });
      };

      if (typeof String.prototype.startsWith != 'function') {
        String.prototype.startsWith = function(str) {
          return this.indexOf(str) === 0;
        };
      }

      Object.size = function(obj) {
        var size = 0, key;
        for (key in obj) {
          if (obj.hasOwnProperty(key)) size++;
        }
        return size;
      };

      // sorts a row by column number
      var sortRowByN = function(objectRow, prefix) {
        var newRow = [];
        // console.log(objectRow);
        for (var i = 1; i <= Object.size(objectRow); i++) {
              newRow[i-1] = objectRow[prefix + i];
        }
        return newRow;
      };

      var getRandomTableId = function(schemas) {
        var randomTableId = '';
        $scope.$watch('schemas', function(newSchemas, oldSchemas) {
          if (newSchemas != oldSchemas) {
            for (var firstSchema in newSchemas) {
              var randFirstSchema = newSchemas[firstSchema];
              var randFirstSchemaName = firstSchema;
                for (var firstTable in randFirstSchema.tables) {
                  var randFirstTableName = firstTable;
                  randomTableId = randFirstSchemaName + '.' + randFirstTableName;
                  break;
                }
              break;
            }
          }
          $scope.tableId = randomTableId;
        }, true);
      };
    }
  ]);
  
  
  dbpresControllers.controller('PaginationCtrl', ['$scope',
    function($scope) {
      $scope.currentPage = 1;
      $scope.maxSize = 5;
      $scope.numPerPage = 10;
      $scope.totalItems = 100000;

      $scope.numPages = function () {
        return Math.ceil($scope.totalItems/$scope.numPerPage);
      };

      $scope.$watch('currentPage + numPerPage', function() {
        var begin = (($scope.currentPage - 1) * $scope.numPerPage);
        var end = begin + $scope.numPerPage;

        // $scope.showTable(begin, end);
      });
    }
  ]);

  dbpresControllers.controller('SidebarController', ['$scope',
    function($scope) {

    }
  ]);

})();