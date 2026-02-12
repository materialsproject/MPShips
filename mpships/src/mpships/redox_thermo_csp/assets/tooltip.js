window.dccFunctions = window.dccFunctions || {};
// make formatting of marks attractive for logaraithmic scales
window.dccFunctions.powerOfTen = function(value) {
    if (value < -2) {
        return parseFloat((10 ** value).toFixed(-value + 2));
    } else {
        return Number.isInteger(10 ** value) ? (10 ** value) : (10 ** value).toFixed(2);
    }

}
