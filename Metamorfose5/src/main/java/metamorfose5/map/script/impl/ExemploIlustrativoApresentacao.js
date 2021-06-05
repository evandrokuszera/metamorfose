function f(values){
    input = JSON.parse(values);
    output = JSON.parse('{}');
    // Begin: User Transformation Logic
    
    output.fname = input.fname.toUpperCase();
    
    // End: User Transformation Logic
    return JSON.stringify(output);
}


