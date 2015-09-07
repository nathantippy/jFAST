package com.ociweb.jfast.stream;

import com.ociweb.pronghorn.pipe.Pipe;


public interface GeneratorDriving {

    int getActiveScriptCursor();
    void setActiveScriptCursor(int cursor);
    
        
    void runBeginMessage();
    void runFromCursor(Pipe mockRB);
    
    int getActiveToken();
    long getActiveFieldId();
    String getActiveFieldName();
    int scriptLength();
    
}
