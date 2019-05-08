package com.ws.ogre.v2.utils;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class FromToValidator implements IParameterValidator {
    public void validate(String theName, String theValue) throws ParameterException {

        if (!theValue.matches("[0-9]{4}-[0-1][0-9]-[0-3][0-9]:[0-2][0-9]")) {
            throw new ParameterException("Parameter " + theName + " should be on format 'yyyy-MM-dd:HH'. (found " + theValue + ")");
        }
    }
}