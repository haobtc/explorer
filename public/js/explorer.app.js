var app = angular.module('myApp', ['ngRoute'])
  .config(function($routeProvider) {
    $routeProvider
      .when('/', {
	controller: 'ActvCtrl',
	templateUrl: 'list1.html'
      })
      .when('/:id', {
	controller: 'ActvCtrl',
	templateUrl: 'list1.html'
      })
      .otherwise({redirectTo: '/'});
  });

app.controller('ActvCtrl', function($scope, $routeParams) {
  $scope.activities = [{
    name: 'zhhh'
  }, {
    name: 'xxx'
  }, {
    name: 'xxxy'
  }];
});
