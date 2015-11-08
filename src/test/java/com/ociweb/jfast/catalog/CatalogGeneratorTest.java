package com.ociweb.jfast.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.jfast.FAST;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeBundle;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.schema.generator.CatalogGenerator;
import com.ociweb.pronghorn.pipe.schema.generator.TemplateGenerator;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;


public class CatalogGeneratorTest {
    
    private static final int FIELD_RANGE_BASE = 1000;

    int[] numericTypes = new int[] {
            TypeMask.IntegerUnsigned,
            TypeMask.IntegerUnsignedOptional,
            TypeMask.IntegerSigned,
            TypeMask.IntegerSignedOptional,
            TypeMask.LongUnsigned,
            TypeMask.LongUnsignedOptional,
            TypeMask.LongSigned,
            TypeMask.LongSignedOptional,
            TypeMask.Decimal,
            TypeMask.DecimalOptional
    };
    
    int[] numericOps = new int[] {
    		OperatorMask.Field_Copy,   
    		OperatorMask.Field_Delta,
    		OperatorMask.Field_None, 
    		OperatorMask.Field_Constant,
            OperatorMask.Field_Default, 
            OperatorMask.Field_Increment,            
    };
    
    int[] varLenOps = new int[] {
            OperatorMask.Field_Constant,
            OperatorMask.Field_Copy,
            OperatorMask.Field_Default,
 //           OperatorMask.Field_Delta,          
            OperatorMask.Field_None,
            OperatorMask.Field_Tail
    };
    
    int[] varLenTypes = new int[] {
            TypeMask.TextASCII,
            TypeMask.TextASCIIOptional,
            TypeMask.TextUTF8,
            TypeMask.TextUTF8Optional,
            TypeMask.ByteArray,
            TypeMask.ByteArrayOptional
    };
    
    
   
    private final int writeBuffer=4096;
    private int testRecordCount = 3;//100;//100000; //testing enough to get repeatable results
    
    private static final int testTemplateId = 2;
    private static final int testMessageIdx = 0;
    private static final String name = "testTemplate";
    
    
    List<String>  numericCatalogXML;
    List<byte[]>  numericCatalogs;
    List<Integer> numericFieldCounts;
    List<Integer> numericFieldTypes;
    List<Integer> numericFieldOperators;
    
    List<String>  textCatalogXML;
    List<byte[]> textCatalogs;
    List<Integer> textFieldCounts;
    List<Integer> textFieldTypes;
    List<Integer> textFieldOperators;
    

    int intDataIndex = ReaderWriterPrimitiveTest.unsignedIntData.length;
    int longDataIndex = ReaderWriterPrimitiveTest.unsignedLongData.length;
    int stringDataIndex = ReaderWriterPrimitiveTest.stringData.length;

    
    //for print of each testing data block to know that the value changed
    private int lastOp = -1;
    private int lastType = -1;
    
    
    public void buildTextCatalogs() {
        textCatalogXML = new ArrayList<String>();
        textCatalogs = new ArrayList<byte[]>();
        textFieldCounts = new ArrayList<Integer>();
        textFieldTypes = new ArrayList<Integer>();
        textFieldOperators = new ArrayList<Integer>();
        
        boolean reset = false;
        String dictionary = null;
                
        
        StringBuilder templateXML = new StringBuilder();
 
        
        //try out zero and non zero initial values in the dictionary
        int initInitialValue = 3;
        int initialValue = initInitialValue;
        
        
            //build initial value for dictionary field
            String fieldInitial;
            if (--initialValue == (initInitialValue-1) ){
               fieldInitial = null;
            } else {
               fieldInitial = Integer.toHexString(initialValue).substring(0, initialValue);
            }
            if (0==initialValue) {
                initialValue = initInitialValue;
            }
            
            int totalFields = Math.max(ReaderWriterPrimitiveTest.stringDataBytes.length, ReaderWriterPrimitiveTest.byteData.length);                         
           
            assert(totalFields>2) : "need room for the presence toggle";
            int p = varLenOps.length;
            while (--p>=0) {            
                int fieldOperator = varLenOps[p];            
                int t = varLenTypes.length;
                while (--t>=0) {               
                    int fieldType = varLenTypes[t];
                    boolean fieldPresence = 0!=(1&fieldType);  
                    
                        int fieldCount = 1; 
                        while (fieldCount<totalFields) {     
                        	templateXML.setLength(0);
                            int f = fieldCount;
                            CatalogGenerator cg = new CatalogGenerator();
                            TemplateGenerator template = cg.addTemplate(name, testTemplateId, reset, dictionary);            
                            
                            int fieldId = 1000;
                            while (--f>=0) {
                                String fieldName = "field"+fieldId;
                                template.addField(fieldName, fieldId++, fieldPresence, fieldType, fieldOperator, fieldInitial);        
                            }
                            int expectedScriptLength = 2+fieldCount;
                            assertTrue(fieldType != TypeMask.Decimal && fieldType != TypeMask.DecimalOptional);
                                            
                            byte[] catBytes = buildCatBytes(templateXML, cg);  
                                                
                            TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);                    
                            assertEquals(1, catalog.templatesCount());
                            
                            assertEquals(expectedScriptLength,catalog.getScriptTokens().length);
                                   
                            textCatalogXML.add(templateXML.toString());
                            textCatalogs.add(catBytes);
                            textFieldCounts.add(new Integer(fieldCount));
                            textFieldTypes.add(new Integer(fieldType));
                            textFieldOperators.add(new Integer(fieldOperator));
                            
                            fieldCount = acceleratedIncrement(fieldCount);
                        }

                }            
            } 

      
        
    }


    private void buildNumericCatalogs() {
        numericCatalogXML = new ArrayList<String>();
        numericCatalogs = new ArrayList<byte[]>();
        numericFieldCounts = new ArrayList<Integer>();
        numericFieldTypes = new ArrayList<Integer>();
        numericFieldOperators = new ArrayList<Integer>();
                
        
        boolean reset = false;
        String dictionary = null;
                
        
        StringBuilder templateXML = new StringBuilder();
        
        //try out zero and non zero initial values in the dictionary
        int initInitialValue = 3;
        int initialValue = initInitialValue;

        String fieldInitial = Integer.toString(--initialValue);
        if (0==initialValue) {
            initialValue = initInitialValue;
        }
        
        int totalFields = Math.max(ReaderWriterPrimitiveTest.unsignedLongData.length, ReaderWriterPrimitiveTest.unsignedIntData.length);                         
       
        assert(totalFields>2) : "need room for the presence toggle";
        int p = numericOps.length;
        while (--p>=0) {            
            int fieldOperator = numericOps[p];            
            int t = numericTypes.length;
            while (--t>=0) {               
                int fieldType = numericTypes[t];
                boolean fieldPresence = 0!=(1&fieldType);  
                
                if (fieldType == TypeMask.Decimal || fieldType == TypeMask.DecimalOptional) {
                    //must try all the second operators with the first for all the decimal combinations.
                    int o2 = numericOps.length;
                    while (--o2>=0) {    
                        int fieldOperator2 = numericOps[o2];      
                        buildAllCatalogsForTypeAndOperation(name, reset,
                                dictionary, templateXML, fieldPresence,
                                fieldInitial, totalFields, fieldOperator, fieldOperator2, fieldType);
                    }
                    
                } else {
                    //for simple case where field only has 1 operator
                    buildAllCatalogsForTypeAndOperation(name, reset,
                            dictionary, templateXML, fieldPresence,
                            fieldInitial, totalFields, fieldOperator, fieldType);
                }

            }            
        } 

    }


    private void buildAllCatalogsForTypeAndOperation(String name,
            boolean reset, String dictionary, StringBuilder templateXML,
            boolean fieldPresence, String fieldInitial, int totalFields, int fieldOperator, int fieldType) {
            
            int fieldCount = 1; 
            while (fieldCount<totalFields) {     
            	templateXML.setLength(0);
                int f = fieldCount;
                CatalogGenerator cg = new CatalogGenerator();
                TemplateGenerator template = cg.addTemplate(name, testTemplateId, reset, dictionary);            
                
                int fieldId = 1000;
                while (--f>=0) {
                    String fieldName = "field"+fieldId;
                    template.addField(fieldName, fieldId++, fieldPresence, fieldType, fieldOperator, fieldInitial);        
                }
                int expectedScriptLength = 2+fieldCount;
                assertTrue(fieldType != TypeMask.Decimal && fieldType != TypeMask.DecimalOptional);
                                
                populateNumericTestData(fieldOperator, fieldType,
                                        fieldCount, templateXML, cg,
                                        expectedScriptLength);
                
                fieldCount = acceleratedIncrement(fieldCount);
            }
            
    }


    private int acceleratedIncrement(int fieldCount) {
        if (fieldCount<4) {
            fieldCount+=1;
        } else if (fieldCount<100) {
            fieldCount+=11;
        } else {
            fieldCount+=111;
        }
        return fieldCount;
    }

    private void buildAllCatalogsForTypeAndOperation(String name,
            boolean reset, String dictionary, StringBuilder templateXML,
            boolean fieldPresence, String fieldInitial, int totalFields, int fieldOperator1, int fieldOperator2, int fieldType) {
            
            int fieldCount = 1; 
            while (fieldCount<totalFields) {     
                templateXML.setLength(0);
                int f = fieldCount;
                CatalogGenerator cg = new CatalogGenerator();
                TemplateGenerator template = cg.addTemplate(name, testTemplateId, reset, dictionary);            
                
                int fieldId = FIELD_RANGE_BASE;
                while (--f>=0) {
                    String fieldName = "field"+fieldId;
                    template.addField(fieldName, fieldId++, fieldPresence, fieldType, fieldOperator1, fieldOperator2, fieldInitial, fieldInitial);        
                }
                int expectedScriptLength = 2+fieldCount;
                expectedScriptLength +=fieldCount;
                assertTrue(fieldType == TypeMask.Decimal || fieldType == TypeMask.DecimalOptional);
                
                populateNumericTestData(fieldOperator1, fieldType,
                                        fieldCount, templateXML, cg,
                                        expectedScriptLength);
                
                fieldCount = acceleratedIncrement(fieldCount);
            }
            
    }
    

    private void populateNumericTestData(int fieldOperator, int fieldType,
            int fieldCount, StringBuilder templateXML, CatalogGenerator cg,
            int expectedScriptLength) {
        byte[] catBytes = buildCatBytes(templateXML, cg);  
                            
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);                    
        assertEquals(1, catalog.templatesCount());
        
        assertEquals(expectedScriptLength,catalog.getScriptTokens().length);
               
        numericCatalogXML.add(templateXML.toString());
        numericCatalogs.add(catBytes);
        numericFieldCounts.add(new Integer(fieldCount));
        numericFieldTypes.add(new Integer(fieldType));
        numericFieldOperators.add(new Integer(fieldOperator));
    }
    
    
    @Test
    public void numericFieldTest() {
        buildNumericCatalogs();
        
        AtomicLong totalWrittenCount = new AtomicLong();
        int i = numericCatalogs.size();
        System.out.println("testing "+i+" numeric configurations");

        while (--i>=0) {
            testEncoding(numericFieldOperators.get(i).intValue(), 
                         numericFieldTypes.get(i).intValue(), 
                         numericFieldCounts.get(i).intValue(), 
                         numericCatalogs.get(i),
                         totalWrittenCount, 
                         numericCatalogXML.get(i),
                         numericCatalogs.size()-i);
        }
        System.err.println("totalWritten:"+totalWrittenCount.longValue());
            
    }
    
    @Test
    public void varLenTypes() {
        buildTextCatalogs();
        
        AtomicLong totalWrittenCount = new AtomicLong();
        int i = textCatalogs.size();
        System.out.println("testing "+i+" text configurations");
        while (--i>=0) {
            testEncoding(textFieldOperators.get(i).intValue(), 
            			 textFieldTypes.get(i).intValue(), 
            			 textFieldCounts.get(i).intValue(), 
            			 textCatalogs.get(i),
                         totalWrittenCount,
                         textCatalogXML.get(i),
                         textCatalogs.size()-i);
        }
        System.err.println("totalWritten:"+totalWrittenCount.longValue());
            
    }

    

    private void testEncoding(int fieldOperator, int fieldType, int fieldCount, byte[] catBytes, AtomicLong totalWritten, String catalogXML, int ordinal) {
        int type = fieldType;
        int operation = fieldOperator;
             
        
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
                
        assertEquals(1, catalog.templatesCount());
        
        FASTClassLoader.deleteFiles();
        
		FieldReferenceOffsetManager from = catalog.getFROM();
        PipeBundle ringBuffers= PipeBundle.buildRingBuffers(new Pipe(new PipeConfig((byte)10, (byte)16, catalog.ringByteConstants(), new MessageSchemaDynamic(from))).initBuffers());
                
       // FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes);
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriterDebug(catBytes);

        if (operation!=lastOp) {
            lastOp = operation;            
            System.err.println(this.getClass().getSimpleName()+" using "+writerDispatch.getClass().getSimpleName()+" testing "+OperatorMask.methodOperatorName[operation]);
            
        }
        
        if (type!=lastType) {
            lastType = type;
           	System.err.println("                                  type:"+TypeMask.methodTypeName[type]+TypeMask.methodTypeSuffix[type]);  
        }

        boolean isOptional = 0!=(1&fieldType);
                        
        //If this test is going to both encode then decode to test both parts of the process this
        //buffer must be very large in order to hold all the possible permutations
        byte[] buffer = new byte[1<<24];
        FASTOutput fastOutput = new FASTOutputByteArray(buffer );
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, true);

        Pipe ringBuffer = PipeBundle.get(ringBuffers,0);
        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, ringBuffer, writerDispatch);
             
        dynamicWriter.reset(true);
        
        //populate ring buffer with the new records to be written.
        resetTestDataPosition();
        float millionPerSecond = timeEncoding(fieldType, fieldCount, ringBuffer, dynamicWriter)/1000000f;    
        
        PrimitiveWriter.flush(writer);
        long bytesWritten = PrimitiveWriter.totalWritten(writer);
        assertEquals(0, PrimitiveWriter.bytesReadyToWrite(writer));
        
        //Use as bases for building single giant test file with test values provided, in ascii?
        totalWritten.addAndGet(PrimitiveWriter.totalWritten(writer));
        
        FASTInput fastInput = new FASTInputByteArray(buffer, (int)bytesWritten);
        

        FASTReaderReactor reactor = FAST.inputReactorDebug(fastInput, catBytes, ringBuffers);
        //FASTReaderReactor reactor = FAST.inputReactor(fastInput, catBytes, ringBuffers);
        
        resetTestDataPosition();

        //lookup all the field LOCs neededed for reading.
        int fieldId = FIELD_RANGE_BASE;
        int TEMPLATE_LOC = FieldReferenceOffsetManager.lookupTemplateLocator(name, from);
        int[] TEMPLATE_FIELDS = new int[fieldCount];
        int t = fieldCount;
        while (--t>=0) {
            TEMPLATE_FIELDS[t] = FieldReferenceOffsetManager.lookupFieldLocator(fieldId++, TEMPLATE_LOC, from);
        }
        

        
        //read all the data off the ring and confirm its what we expected.
        Pipe[] buffers = reactor.ringBuffers();
        int buffersCount = buffers.length;
        try {
            int constIntValue = Integer.MIN_VALUE;
            long constLongValue = Integer.MIN_VALUE;
            
	        int j = testRecordCount;
	        while (j>0 && FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
	        	int k = buffersCount;
	        	while (j>0 && --k>=0) {
	        		
	        		if (PipeReader.tryReadFragment(buffers[k])) {
	        		    	        		    
						assertTrue(PipeReader.isNewMessage(buffers[k]));
	        			assertEquals(testMessageIdx, PipeReader.getMsgIdx(buffers[k]));
	        			
	        			int f = fieldCount;
	        			while (--f>=0) {
	        			    
	        			    switch (fieldType) {
    	        			    case TypeMask.IntegerUnsigned:
    	        			    case TypeMask.IntegerUnsignedOptional:
    	        			    case TypeMask.IntegerSigned:
    	        			    case TypeMask.IntegerSignedOptional:
    	        			        
    	        			        int expectedIntValue = ReaderWriterPrimitiveTest.unsignedIntData[--intDataIndex];
    	        			        int actualIntValue = PipeReader.readInt(buffers[k], TEMPLATE_FIELDS[f]);
    	        			        
    	        			        if (fieldWillBeAbsent(isOptional, j-1, f)) {
    	        			            //the last field of even records
    	        			            expectedIntValue = TokenBuilder.absentValue32(TokenBuilder.MASK_ABSENT_DEFAULT);
    	        			        } else 
    	        			            if (OperatorMask.Field_Constant==fieldOperator) {
    	        			            //just checking that the value never changes because the same constant eg initialValue is used for the catalog message
    	        			            if (constIntValue==Integer.MIN_VALUE) {
    	        			                assert(!fieldWillBeAbsent(isOptional, j-1, f));
    	        			                constIntValue = actualIntValue;
    	        			            }
    	        			            expectedIntValue = constIntValue;	        			                
    	        			        } 
    	        			        
    	        			        
    	        			        if (expectedIntValue!=actualIntValue) {
    	        			            System.err.println(catalogXML);
    	        			            fail("idx:"+intDataIndex+" Expected "+expectedIntValue+" but found "+actualIntValue);
    	        			        }
    	        			        
    	        			        if (0 == intDataIndex) {
    	        			            intDataIndex = ReaderWriterPrimitiveTest.unsignedIntData.length;
    	        			        }	        			            	        			            
    	        			        
    	        			        break;
    	        			        
                                case TypeMask.LongUnsigned:
                                case TypeMask.LongUnsignedOptional:
                                case TypeMask.LongSigned:
                                case TypeMask.LongSignedOptional:
                                    
                                    long expectedLongValue = ReaderWriterPrimitiveTest.unsignedLongData[--longDataIndex];
                                    long actualLongValue = PipeReader.readLong(buffers[k], TEMPLATE_FIELDS[f]);
                                    
//                                    if (fieldWillBeAbsent(isOptional, j-1, f)) {
//                                        //the last field of even records
//                                        expectedLongValue = TokenBuilder.absentValue64(TokenBuilder.MASK_ABSENT_DEFAULT);
//                                    } else 
                                        if (OperatorMask.Field_Constant==fieldOperator) {
                                        //just checking that the value never changes because the same constant eg initialValue is used for the catalog message
                                        if (constLongValue==Integer.MIN_VALUE) {
                                           // assert(!fieldWillBeAbsent(isOptional, j-1, f));
                                            constLongValue = actualLongValue;
                                        }
                                        expectedLongValue = constLongValue;                                      
                                    } 
                                    
                                    
                                    if (expectedLongValue!=actualLongValue) {
                                        System.err.println(catalogXML);
                                        fail("idx:"+longDataIndex+" Expected "+expectedLongValue+" but found "+actualLongValue);
                                    }
                                    
                                    if (0 == longDataIndex) {
                                        longDataIndex = ReaderWriterPrimitiveTest.unsignedLongData.length;
                                    }                                                                       
                                    
                                    break;
                                    
                                case TypeMask.ByteArray:
                                case TypeMask.ByteArrayOptional:
                                case TypeMask.TextASCII:
                                case TypeMask.TextASCIIOptional:
                                case TypeMask.TextUTF8:
                                case TypeMask.TextUTF8Optional:
                                    
                                    
    	        			   default:
    	        			       //nothing
	        			    
	        			    }
	        			    
	        			    
	        			    
	        			    
	        			    
	        			    
	        			    
	        			}
	        			
	        			PipeReader.releaseReadLock(buffers[k]);
	        			
	        			j--;
	        		}        		
	        	}       	
	        }
	        //confirm that the internal stacks have gone back down to zero
	        assertEquals(0,writer.safetyStackDepth);
	        assertEquals(0,writer.flushSkipsIdxLimit);
	        
        } catch (Exception ex) {
        	
            System.err.println(catalogXML);
        	
            //dump exactly what was written
	        int limit = (int) Math.min(bytesWritten, 55);
	        int q = 0;
	        while (q<limit) {
	          	System.err.println(q+"   "+byteString(buffer[q]));
	          	q++;
	          	
	        }
            
        	throw new RuntimeException(ex);
        }

    }


    private boolean fieldWillBeAbsent(boolean isOptional, int j, int f) {
        return isOptional && 0==(j&1) && 0==f;
    }


    private void resetTestDataPosition() {
        intDataIndex = ReaderWriterPrimitiveTest.unsignedIntData.length;
        longDataIndex = ReaderWriterPrimitiveTest.unsignedLongData.length;
        stringDataIndex = ReaderWriterPrimitiveTest.stringData.length;
    }


	private String byteString(int value) {
		String tmp = "00000000"+Integer.toBinaryString(value);
		return tmp.substring(tmp.length()-8, tmp.length());
	}

	 
    private float timeEncoding(int fieldType, final int fieldCount, Pipe ringBuffer, FASTDynamicWriter dynamicWriter) {

    	    	
        boolean isOptional = 0!=(1&fieldType);
        
    	int i = testRecordCount;
        switch(fieldType) {
            case TypeMask.IntegerUnsigned:
            case TypeMask.IntegerSigned:
            case TypeMask.IntegerUnsignedOptional:
            case TypeMask.IntegerSignedOptional:
                
                {                 	
                    long start = System.nanoTime();
                    
                    while (--i>=0) {
                        Pipe.addMsgIdx(ringBuffer, testMessageIdx);
                        
                        int j = fieldCount;
                        while (--j>=0) {
                            int value = ReaderWriterPrimitiveTest.unsignedIntData[--intDataIndex];
                            
                            if (fieldWillBeAbsent(isOptional, i, j)) {
                                //the last field of even records
                                value = TokenBuilder.absentValue32(TokenBuilder.MASK_ABSENT_DEFAULT);
                            }
                            
                            Pipe.addIntValue(value, ringBuffer);
//                            long whp = Pipe.workingHeadPosition(ringBuffer);       //delete me three                     
//                            Pipe.setValue(Pipe.primaryBuffer(ringBuffer),ringBuffer.mask,whp,value);
//                            Pipe.setWorkingHead(ringBuffer, whp+1);
                            
                            if (0 == intDataIndex) {
                                intDataIndex = ReaderWriterPrimitiveTest.unsignedIntData.length;
                            }
                            
                        }
                        Pipe.publishWrites(ringBuffer);
                        
                        if (PipeReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                        	
                        	PipeReader.getMsgIdx(ringBuffer);
                        	
                            FASTDynamicWriter.write(dynamicWriter);
                            PipeReader.releaseReadLock(ringBuffer);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                }
            case TypeMask.LongUnsigned:
            case TypeMask.LongUnsignedOptional:
            case TypeMask.LongSigned:
            case TypeMask.LongSignedOptional:
                { 
                    long start = System.nanoTime();
                    
                  
                    while (--i>=0) {
                    	Pipe.addMsgIdx(ringBuffer, testMessageIdx);
                    	 
                        int j = fieldCount;
                        while (--j>=0) {
                            long value = ReaderWriterPrimitiveTest.unsignedLongData[--longDataIndex];
                            
//                            if (fieldWillBeAbsent(isOptional, i, j)) {
//                                //the last field of even records
//                                value = TokenBuilder.absentValue64(TokenBuilder.MASK_ABSENT_DEFAULT);
//                            }
                            
                            Pipe.addLongValue(value, ringBuffer);
                            if (0==longDataIndex) {
                                longDataIndex = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        Pipe.publishWrites(ringBuffer);
                        if (PipeReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                            FASTDynamicWriter.write(dynamicWriter);
                            PipeReader.releaseReadLock(ringBuffer);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                }
            case TypeMask.Decimal:
            case TypeMask.DecimalOptional:
                {
                    long start = System.nanoTime();
                    
                    int exponent = 2;
          
                    while (--i>=0) {
                        Pipe.addMsgIdx(ringBuffer, testMessageIdx);
                        int j = fieldCount;
                        while (--j>=0) {
                            Pipe.addDecimal(exponent, ReaderWriterPrimitiveTest.unsignedLongData[--longDataIndex],ringBuffer);
                            if (0==longDataIndex) {
                                longDataIndex = ReaderWriterPrimitiveTest.unsignedLongData.length;
                            }
                        }
                        Pipe.publishWrites(ringBuffer);
                        if (PipeReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                            FASTDynamicWriter.write(dynamicWriter);
                            PipeReader.releaseReadLock(ringBuffer);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                } 
            case TypeMask.TextASCII:
            case TypeMask.TextASCIIOptional:
            case TypeMask.TextUTF8:
            case TypeMask.TextUTF8Optional:
                {
                    long start = System.nanoTime();
                    
                    int exponent = 2;
                   
      
                    while (--i>=0) {
                        Pipe.addMsgIdx(ringBuffer, testMessageIdx);
                        int j = fieldCount;
                        while (--j>=0) {
                            //TODO: B, this test is not using UTF8 encoding for the UTF8 type mask!!!! this is only ASCII enoding always.
                            byte[] source = ReaderWriterPrimitiveTest.stringDataBytes[--stringDataIndex];
							Pipe.addByteArray(source, 0, source.length, ringBuffer);
                            if (0==stringDataIndex) {
                                stringDataIndex = ReaderWriterPrimitiveTest.stringData.length;
                            }
                        }
                        Pipe.publishWrites(ringBuffer);
                        if (PipeReader.tryReadFragment(ringBuffer)) {//without move next we get no stats.
                            FASTDynamicWriter.write(dynamicWriter);
                            PipeReader.releaseReadLock(ringBuffer);
                        }
                    }
                    long duration = System.nanoTime()-start;
                    return 1000000000f*testRecordCount/duration;
                    
                }
                
        
        }
        return 0;
    }



    private byte[] buildCatBytes(StringBuilder builder, CatalogGenerator cg) {
        try {
			builder = (StringBuilder) cg.appendTo("", builder);
			boolean debug = false;
			if (debug) {
				System.err.println(builder);
			}			
			
			ClientConfig clientConfig = new ClientConfig();  //keep bits small or the test will take a very long time to run.              
			byte[] catBytes = TemplateLoader.buildCatBytes(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)), clientConfig);
			return catBytes;
		} catch (IOException e) {
			throw new FASTException(e);
		}
    }



    public static byte[] buildRawCatalogData(ClientConfig clientConfig) {
        //this example uses the preamble feature
        clientConfig.setPreableBytes((short)4);

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, "/performance/example.xml", clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }
    
    
    

}
