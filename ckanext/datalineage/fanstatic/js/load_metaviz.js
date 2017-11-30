// Enable JavaScript's strict mode. Strict mode catches some common
// programming errors and throws exceptions, prevents some unsafe actions from
// being taken, and disables some confusing and bad JavaScript features.
"use strict";

ckan.module('datalineage_js_module', function ($) {
  return {
    initialize: function () {
      console.log("I've been initialized for element: ", this.el);
      var data = this.options.data;
      metaViz.displayMetaViz(data);
    }
  };
});