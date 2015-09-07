package com.ociweb.jfast.stream;

import com.ociweb.pronghorn.pipe.Pipe;

public interface FASTListener {

    void fragment(int templateId, Pipe buffer);
    void fragment();
    
}
