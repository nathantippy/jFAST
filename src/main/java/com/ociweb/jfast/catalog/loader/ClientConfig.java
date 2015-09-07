package com.ociweb.jfast.catalog.loader;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;

public class ClientConfig {

    private short preambleBytes;
    
    private int bytesLengthMax = 4096;
    private int bytesGap = 64;
    
    private static final int NONE = -1;
    private int catalogId = NONE;

    public ClientConfig() {
    }
    
    public ClientConfig(int bytesLength, int bytesGap) {
        this.bytesLengthMax = bytesLength;
        this.bytesGap = bytesGap;
        
    }
    
    public ClientConfig(PrimitiveReader reader) {
        
        preambleBytes = (short)PrimitiveReader.readIntegerUnsigned(reader);
        
        bytesLengthMax = PrimitiveReader.readIntegerUnsigned(reader);
        bytesGap = PrimitiveReader.readIntegerUnsigned(reader);
        
        catalogId = PrimitiveReader.readIntegerSigned(reader);

    }

    public void save(PrimitiveWriter writer) {
        
        PrimitiveWriter.writeIntegerUnsigned(preambleBytes, writer);

        PrimitiveWriter.writeIntegerUnsigned(bytesLengthMax, writer);
        PrimitiveWriter.writeIntegerUnsigned(bytesGap, writer);
        
        PrimitiveWriter.writeIntegerSigned(catalogId, writer);
    
        
    }
    
    public short getPreableBytes() {
        return preambleBytes;
    }

    public void setPreableBytes(short preableBytes) {
        this.preambleBytes = preableBytes;
    }
    
    public int getBytesLength() {
        return this.bytesLengthMax;
    }
    
    public int getBytesGap() {
        return this.bytesGap;
    }

    public void setCatalogTemplateId(int id) {
        if (NONE == id) {
            throw new FASTException("Catalog template Id may not be: "+NONE);
        }
        catalogId = id;
    }

}
