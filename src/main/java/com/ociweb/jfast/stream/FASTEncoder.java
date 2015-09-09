package com.ociweb.jfast.stream;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArrayEquals;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.schema.loader.DictionaryFactory;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.LocalHeap;

public abstract class FASTEncoder { 
    
    public int templateStackHead = 0;
    protected final int[] templateStack;

    protected final int instanceBytesMask;
    
    public final int[] rIntDictionary; //rIntDictionary
    protected final int[] intInit;
    public final int intInstanceMask;
    
    public final long[] rLongDictionary; //rLongDictionary
    protected final long[] longInit;
    public final int longInstanceMask;
    
    protected final int nonTemplatePMapSize;
    protected final int templatePMapSize;

    protected int readFromIdx = -1;

    public final DictionaryFactory dictionaryFactory;
    protected final int[][] dictionaryMembers;

    protected DispatchObserver observer;
    public int activeScriptCursor;

    
    protected final int[] fullScript;
    public final long[] fieldIdScript;
    
    public final LocalHeap byteHeap;

    public int fieldPos = -1;
    
    protected RingCharSequence ringCharSequence = new RingCharSequence();
    protected static final int INIT_VALUE_MASK = 0x80000000;
    protected final int TEXT_INSTANCE_MASK;
    
    public final byte[] preambleData;
    
    public FASTEncoder(TemplateCatalogConfig catalog) {
        this(catalog.dictionaryFactory(), catalog.templatesCount(),
             catalog.maxNonTemplatePMapSize(), catalog.maxTemplatePMapSize(), catalog.dictionaryResetMembers(),
             catalog.fullScript(), catalog.fieldIdScript(), catalog.getMaxGroupDepth(), catalog.clientConfig().getPreableBytes());
    }
    
    
    public FASTEncoder(DictionaryFactory dcr, int maxTemplates, int nonTemplatePMapSize, int templatePMapSize,
                                int[][] dictionaryMembers, int[] fullScript, long[] fieldIdScript,
                                int maxNestedGroupDepth, int preambleBytes) {

        this.fullScript = fullScript;
        this.dictionaryFactory = dcr;
        
        //FieldReferenceOffsetManager.printScript("debug encoder ", fullScript);
        
        this.fieldIdScript = fieldIdScript;
               
        this.nonTemplatePMapSize = nonTemplatePMapSize;
        this.templatePMapSize = templatePMapSize;

        this.rIntDictionary = dcr.integerDictionary();
        this.intInit = dcr.integerDictionary();
        assert (rIntDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rIntDictionary.length));
        this.intInstanceMask = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        
        this.rLongDictionary = dcr.longDictionary();
        this.longInit = dcr.longDictionary();
        assert (rLongDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rLongDictionary.length));
        this.longInstanceMask = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        
        this.byteHeap = dcr.byteDictionary();

        this.TEXT_INSTANCE_MASK = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (LocalHeap.itemCount(byteHeap) - 1));
        this.instanceBytesMask = null==byteHeap? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (LocalHeap.itemCount(byteHeap)-1));

        this.templateStack = new int[maxTemplates];
        this.dictionaryMembers = dictionaryMembers;
        this.preambleData = new byte[preambleBytes];
    }
    


    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }


    public static boolean notifyFieldPositions(PrimitiveWriter writer, int activeScriptCursor) {
        
        if (null!=writer && writer.output instanceof FASTOutputByteArrayEquals) {
            FASTOutputByteArrayEquals testingOutput = (FASTOutputByteArrayEquals)writer.output;
            testingOutput.recordPosition(writer.limit,activeScriptCursor);
        }
    
        return true;
    }
    
    public abstract void encode(PrimitiveWriter writer, Pipe pipe);
    

    public void setActiveScriptCursor(int cursor) {   	
    	
		assert(TypeMask.Group == TokenBuilder.extractType(fullScript[cursor])) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(fullScript[cursor]);
		assert((OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(fullScript[cursor])) == 0) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(fullScript[cursor]);

       activeScriptCursor = cursor;
    }
    
    public int getActiveScriptCursor() {
        return activeScriptCursor;
    }
    
    
}
