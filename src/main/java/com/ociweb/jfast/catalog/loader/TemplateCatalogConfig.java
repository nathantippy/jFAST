//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.catalog.loader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.schema.loader.DictionaryFactory;
import com.ociweb.pronghorn.ring.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.ring.util.hash.LongHashTable;
import com.ociweb.pronghorn.ring.util.hash.LongHashTableVisitor;

public class TemplateCatalogConfig {

    ///////////
    //properties that should only be set after reading the documentation.
    //Change impact generated code so the changes must be done only once before the catBytes are generated.
    ///////////
    
    public final ClientConfig clientConfig;
    
    
    private final DictionaryFactory dictionaryFactory;
    private final int maxTemplatePMapSize;
    private final int maxNonTemplatePMapSize;
    private final int maxPMapDepth;
    private final int maxFieldId; 
 
    private final LongHashTable templateToStartIdx;
    
    public final int[] templateScriptEntries;
    public final int[] templateScriptEntryLimits;

    final int[] scriptTokens;
    final long[] scriptFieldIds;
    private final String[] scriptFieldNames;
    private final String[] scriptDictionaryNames;
    private final int templatesInCatalog;

    
    private final int[][] dictionaryMembers;

    private final FieldReferenceOffsetManager from;
    
    public TemplateCatalogConfig(byte[] catBytes) {
        
        FASTInputStream inputStream;
        try {
            inputStream = new FASTInputStream(new GZIPInputStream(new ByteArrayInputStream(catBytes),catBytes.length));
        } catch (IOException e) {
            throw new FASTException(e);
        }
        
        
        PrimitiveReader reader = new PrimitiveReader(1024, inputStream, 0);

        //given an index in the script lookup the tokens, fieldIds or fieldNames
        int fullScriptLength = PrimitiveReader.readIntegerUnsigned(reader);
                        
        scriptTokens = new int[fullScriptLength];
        scriptFieldIds = new long[fullScriptLength];
        scriptFieldNames = new String[fullScriptLength];
        scriptDictionaryNames = new String[fullScriptLength];
        
        //given the template id from the template file look up the 
        //script starts and limits
        templatesInCatalog = PrimitiveReader.readIntegerUnsigned(reader);
        
        //how many bits would be needed to store this many templateIds
        int bitsForTemplates = 32 - Integer.numberOfLeadingZeros(templatesInCatalog - 1);
        
        templateToStartIdx = new LongHashTable(bitsForTemplates+2); //add 2 so we have plenty of extra room for hashes
        
        
        templateScriptEntries = new int[templatesInCatalog];
        templateScriptEntryLimits = new int[templatesInCatalog];
        
        loadTemplateScripts(reader);

        int dictionaryCount = PrimitiveReader.readIntegerUnsigned(reader);
        dictionaryMembers = new int[dictionaryCount][];

        loadDictionaryMembers(reader);

        maxFieldId = PrimitiveReader.readIntegerUnsigned(reader);
        // it is assumed that template PMaps are smaller or larger than the
        // other PMaps so these are kept separate
        maxTemplatePMapSize = PrimitiveReader.readIntegerUnsigned(reader);
        maxNonTemplatePMapSize = PrimitiveReader.readIntegerUnsigned(reader);
        maxPMapDepth = PrimitiveReader.readIntegerUnsigned(reader);
		DictionaryFactory df = new DictionaryFactory();
		TemplateCatalogConfig.load(df,reader);

        dictionaryFactory = df;
                
        clientConfig = new ClientConfig(reader);
        
        //must be done after the client config construction
        from = TemplateCatalogConfig.createFieldReferenceOffsetManager(this);
        
    }
    
    @Deprecated //for testing only
    public TemplateCatalogConfig(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
                                int[] fullScript, int maxNestedGroupDepth,
                                int maxTemplatePMapSize, int templatesCount, ClientConfig clientConfig) {
        
        this.scriptTokens = fullScript;
        this.maxNonTemplatePMapSize  = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;
        this.maxPMapDepth = maxNestedGroupDepth;
        this.templatesInCatalog=templatesCount;
        this.templateToStartIdx=null;
        this.scriptFieldNames=null;
        this.scriptDictionaryNames=null;
        this.templateScriptEntries=null;
        this.templateScriptEntryLimits=null;
        this.scriptFieldIds=null;
        this.maxTemplatePMapSize = maxTemplatePMapSize;
        this.maxFieldId=-1;
        this.dictionaryFactory = dcr;
        this.clientConfig = clientConfig;
        
        this.from = TemplateCatalogConfig.createFieldReferenceOffsetManager(this);

    }

	public byte[] ringByteConstants() {
		return DictionaryFactory.initConstantByteArray(dictionaryFactory);
	}

    // Assumes that the tokens are already loaded and ready for use.
    private void loadTemplateScripts(PrimitiveReader reader) {

        int i = templatesInCatalog;
        while (--i >= 0) {
            // look up for script index given the templateId
            long templateId = PrimitiveReader.readLongUnsigned(reader);
            LongHashTable.setItem(templateToStartIdx, templateId, templateScriptEntries[i] = PrimitiveReader.readIntegerUnsigned(reader));
            templateScriptEntryLimits[i] = PrimitiveReader.readIntegerUnsigned(reader);
        }
        
        //Must be ordered in order to be useful 
        Arrays.sort(templateScriptEntries);
        Arrays.sort(templateScriptEntryLimits);

        StringBuilder builder = new StringBuilder();//TODO: B, this is now producing garbage! Temp space must be held by temp space owner!
        i = getScriptTokens().length;
        while (--i >= 0) {
            getScriptTokens()[i] = PrimitiveReader.readIntegerSigned(reader);
            scriptFieldIds[i] = PrimitiveReader.readIntegerUnsigned(reader);
            scriptFieldNames[i] = readUTF8(reader, builder);;
            scriptDictionaryNames[i] = readUTF8(reader, builder);;
            
        }

        // System.err.println("script tokens/fields "+scriptTokens.length);//46
        // System.err.println("templateId idx start/stop count "+this.templateStartIdx.length);//128

    }

	public String readUTF8(PrimitiveReader reader, StringBuilder builder) {
		int len = PrimitiveReader.readIntegerUnsigned(reader);
		String name ="";
		if (len>0) {
		    builder.setLength(0);
		    {
		        byte[] tmp = new byte[len];                    
		        PrimitiveReader.readByteData(tmp,0,len,reader); //read bytes into array
		        
		        long charAndPos = 0;  //convert bytes to chars
		        while (charAndPos>>32 < len  ) { 
		            charAndPos = RingBuffer.decodeUTF8Fast(tmp, charAndPos, Integer.MAX_VALUE);
		            builder.append((char)charAndPos);

		        }
		    }
		    name = builder.toString();
		}
		return name;
	}

    // // stream message* | block*
    // // block BlockSize message+
    // // message segment
    // // segment PresenceMap TemplateIdentifier? (field | segment)*
    // //* field integer | string | delta | ScaledNumber | ByteVector
    // //* integer UnsignedInteger | SignedInteger
    // //* string ASCIIString | UnicodeString
    // //* delta IntegerDelta | ScaledNumberDelta | ASCIIStringDelta
    // |ByteVectorDelta

    public static void save(PrimitiveWriter writer, int biggestId, int uniqueTemplateIds, long biggestTemplateId,
            DictionaryFactory df, int maxTemplatePMap, int maxNonTemplatePMap, int[][] tokenIdxMembers,
            int[] tokenIdxMemberHeads, int[] catalogScriptTokens, long[] catalogScriptFieldIds, String[] catalogScriptFieldNames, String[] dictionaryNames,
            int scriptLength,  LongHashTable templateToOffset, LongHashTable templateToLimit , int maxPMapDepth, ClientConfig clientConfig) {    
        
        saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, catalogScriptTokens, 
                catalogScriptFieldIds, catalogScriptFieldNames, dictionaryNames,
                scriptLength, templateToOffset, templateToLimit);

        saveDictionaryMembers(writer, tokenIdxMembers, tokenIdxMemberHeads);

        PrimitiveWriter.writeIntegerUnsigned(biggestId, writer);
        // System.err.println("save pmap sizes "+maxTemplatePMap+" "+maxNonTemplatePMap);
        PrimitiveWriter.writeIntegerUnsigned(maxTemplatePMap, writer);
        PrimitiveWriter.writeIntegerUnsigned(maxNonTemplatePMap, writer);
        PrimitiveWriter.writeIntegerUnsigned(maxPMapDepth, writer);

        TemplateCatalogConfig.save(df, writer);        
        clientConfig.save(writer);

    }

    private static void saveProperties(PrimitiveWriter writer, Properties properties) {
                
        Set<String> keys = properties.stringPropertyNames();
        PrimitiveWriter.writeIntegerUnsigned(keys.size(), writer);
        for(String key: keys) {
            PrimitiveWriter.writeIntegerUnsigned(key.length(), writer);
            PrimitiveWriter.ensureSpace(key.length(),writer);
            
            {
                //convert from chars to bytes
                //writeByteArrayData()
                int len = key.length();
                int limit = writer.limit;
                int c = 0;
                while (c < len) {
                    limit = RingBuffer.encodeSingleChar((int) key.charAt(c++), writer.buffer, 0xFFFFFFFF, limit);
                }
                writer.limit = limit;
            }
            
            String prop = properties.getProperty(key);
            PrimitiveWriter.writeIntegerUnsigned(prop.length(), writer);
            PrimitiveWriter.ensureSpace(prop.length(),writer);
            
            {
                //convert from chars to bytes
                //writeByteArrayData()
                int len = prop.length();
                int limit = writer.limit;
                int c = 0;
                while (c < len) {
                    limit = RingBuffer.encodeSingleChar((int) prop.charAt(c++), writer.buffer, 0xFFFFFFFF, limit);
                }
                writer.limit = limit;
            }
            
        }
        
    }

    private static void saveDictionaryMembers(PrimitiveWriter writer, int[][] tokenIdxMembers, int[] tokenIdxMemberHeads) {
        // save count of dictionaries
        int dictionaryCount = tokenIdxMembers.length;
        PrimitiveWriter.writeIntegerUnsigned(dictionaryCount, writer);
        //
        int d = dictionaryCount;
        while (--d >= 0) {
            int[] members = tokenIdxMembers[d];
            int h = tokenIdxMemberHeads[d];
            PrimitiveWriter.writeIntegerUnsigned(h, writer);// length of reset script (eg member list)
            while (--h >= 0) {
                PrimitiveWriter.writeIntegerSigned(members[h], writer);
            }
        }
    }

    private void loadDictionaryMembers(PrimitiveReader reader) {
        // //target int[][] dictionaryMembers
        int dictionaryCount = dictionaryMembers.length;
        int d = dictionaryCount;
        while (--d >= 0) {
            int h = PrimitiveReader.readIntegerUnsigned(reader);// length of reset script (eg member list)
            int[] members = new int[h];
            while (--h >= 0) {
                members[h] = PrimitiveReader.readIntegerSigned(reader);
            }
            dictionaryMembers[d] = members;
        }
    }

    /**
     * 
     * Save template scripts to the catalog file. The Script is made up of the
     * field id(s) or Tokens. Each field value needs to know the id so it is
     * stored by id. All other types (group tasks,dictionary tasks) just need to
     * be executed so they are stored as tokens only. These special tasks
     * frequently multiple tokens to a single id which requires that the token
     * is used in all cases. An example is the Open and Close tokens for a given
     * group.
     * 
     * 
     * @param writer
     * @param uniqueTemplateIds
     * @param biggestTemplateId
     * @param catalogScriptFieldNames 
     * @param scripts
     */
    private static void saveTemplateScripts(final PrimitiveWriter writer, int uniqueTemplateIds, long biggestTemplateId,
            int[] catalogScriptTokens, long[] catalogScriptFieldIds, String[] catalogScriptFieldNames, String[] dictionaryNames,
            int scriptLength, LongHashTable templateToOffset, final LongHashTable templateToLimit ) {
        // what size array will we need for template lookup. this must be a
        // power of two
        // therefore we will only store the exponent given a base of two.
        // this is not so much for making the file smaller but rather to do the
        // computation
        // now instead of at runtime when latency is an issue.

        PrimitiveWriter.writeIntegerUnsigned(scriptLength, writer);

        // total number of templates are are defining here in the catalog
        PrimitiveWriter.writeIntegerUnsigned(uniqueTemplateIds, writer);
        // write each template index
        
        LongHashTable.visit(templateToOffset, new LongHashTableVisitor() {

			@Override
			public void visit(long key, int value) {
				
				PrimitiveWriter.writeLongUnsigned(key, writer); 
				// return the index to its original value (-1)
                PrimitiveWriter.writeIntegerUnsigned(value - 1, writer);
                PrimitiveWriter.writeLongUnsigned(LongHashTable.getItem(templateToLimit, key), writer);
				
			}} );
               

        // write the scripts
        int i = scriptLength;
        while (--i >= 0) {
            PrimitiveWriter.writeIntegerSigned(catalogScriptTokens[i], writer);
            PrimitiveWriter.writeLongUnsigned(catalogScriptFieldIds[i], writer); 
           
            writeUTF8(writer, catalogScriptFieldNames[i]);
            writeUTF8(writer, dictionaryNames[i]);
            
        }
    }

	public static void writeUTF8(final PrimitiveWriter writer, String name) {
		int len = null==name?0:name.length();
		PrimitiveWriter.writeIntegerUnsigned(len, writer);
		if (len>0) {
		    PrimitiveWriter.ensureSpace(name.length(),writer);
		    
		    //convert from chars to bytes
		    //writeByteArrayData()
		    int len1 = name.length();
		    int limit = writer.limit;
		    int c = 0;
		    while (c < len1) {
		        limit = RingBuffer.encodeSingleChar((int) name.charAt(c++), writer.buffer, 0xFFFFFFFF, limit);
		    }
		    writer.limit = limit;
		}
	}

    public DictionaryFactory dictionaryFactory() {
        return dictionaryFactory;
    }
   
    public int maxTemplatePMapSize() {
        return maxTemplatePMapSize;
    }

    public int maxFieldId() {
        return maxFieldId;
    }

    public int[][] dictionaryResetMembers() {
        return dictionaryMembers;
    }

    public ClientConfig clientConfig() {
        return clientConfig;
    }

    public int templatesCount() {
        return templatesInCatalog;
    }

    public int[] fullScript() {
        return getScriptTokens();
    }
    public long[] fieldIdScript() {
        return scriptFieldIds;
    }
    public String[] fieldNameScript() {
        return scriptFieldNames;
    }


    public int maxNonTemplatePMapSize() {
        return maxNonTemplatePMapSize;
    }

    public int getMaxGroupDepth() {
        return maxPMapDepth;
    }

    public LongHashTable getTemplateStartIdx() {
        return templateToStartIdx;
    }

    public int[] getScriptTokens() {
        return scriptTokens;
    }

    public FieldReferenceOffsetManager getFROM() {
        return from;
    }

    public static RingBuffers buildRingBuffers(TemplateCatalogConfig catalog, byte primaryBits, byte secondaryBits) {
		return RingBuffers.buildRingBuffers(new RingBuffer(new RingBufferConfig(primaryBits, secondaryBits, catalog.ringByteConstants(), catalog.getFROM())).initBuffers());
	}
    
	public static FieldReferenceOffsetManager createFieldReferenceOffsetManager(TemplateCatalogConfig config) {
		
		//the scriptTokens array is too long and must be shortened, this will cause some garbage once but the length is important
		int tokenCount = 0;
		if (null!=config.scriptTokens) {//for teting
			while (tokenCount<config.scriptTokens.length && config.scriptTokens[tokenCount]<0) {
				tokenCount++;
			}		
			if (0 == tokenCount || 0 != config.scriptTokens[config.scriptTokens.length-1] ) {//this hack is here for testing
				tokenCount = config.scriptTokens.length;
			}
		}
		
		return new FieldReferenceOffsetManager(   null==config.scriptTokens? null : Arrays.copyOfRange(config.scriptTokens, 0, tokenCount), 
									        	  config.clientConfig.getPreableBytes(), 
									              config.fieldNameScript(),
									              config.fieldIdScript(),
									              config.dictionaryScript(),
									              "Catalog");
		
		
	}

	private String[] dictionaryScript() {
		return scriptDictionaryNames;
	}

	public static void load(DictionaryFactory df, PrimitiveReader reader) {
	
		df.singleBytesSize = PrimitiveReader.readIntegerUnsigned(reader);
		df.gapBytesSize = PrimitiveReader.readIntegerUnsigned(reader);
		
		df.integerCount = PrimitiveReader.readIntegerUnsigned(reader);
		df.longCount = PrimitiveReader.readIntegerUnsigned(reader);
		df.bytesCount = PrimitiveReader.readIntegerUnsigned(reader);
	
		df.integerInitCount = PrimitiveReader.readIntegerUnsigned(reader);
		df.integerInitIndex = new int[df.integerInitCount];
		df.integerInitValue = new int[df.integerInitCount];
	    int c = df.integerInitCount;
	    while (--c >= 0) {
	    	df.integerInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
	    	df.integerInitValue[c] = PrimitiveReader.readIntegerSigned(reader);
	    }
	
	    df.longInitCount = PrimitiveReader.readIntegerUnsigned(reader);
	    df.longInitIndex = new int[df.longInitCount];
	    df.longInitValue = new long[df.longInitCount];
	    c = df.longInitCount;
	    while (--c >= 0) {
	    	df.longInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
	    	df.longInitValue[c] = PrimitiveReader.readLongSigned(reader);
	    }
	
	
	    df.byteInitCount = PrimitiveReader.readIntegerUnsigned(reader);
	    df.byteInitIndex = new int[df.byteInitCount];
	    df.byteInitValue = new byte[df.byteInitCount][];
	    c = df.byteInitCount;
	    while (--c >= 0) {
	    	df.byteInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
	        int len = PrimitiveReader.readIntegerUnsigned(reader);
	        if (len>0) {
	        	byte[] value = new byte[len];
	        	PrimitiveReader.readByteData(value, 0, len, reader);
	        	df.byteInitValue[c] = value;
	        } else {
	        	if (len<0) {
	        		df.byteInitValue[c]=null;
	        	} else {
	        		df.byteInitValue[c]=new byte[0];
	        	}
	        }
	    }
	    df.byteInitTotalLength = PrimitiveReader.readIntegerUnsigned(reader);
	
	}

	public static void save(DictionaryFactory df, PrimitiveWriter writer) {
	
		PrimitiveWriter.writeIntegerUnsigned(df.singleBytesSize, writer);
		PrimitiveWriter.writeIntegerUnsigned(df.gapBytesSize, writer);
		
	    PrimitiveWriter.writeIntegerUnsigned(df.integerCount, writer);
	    PrimitiveWriter.writeIntegerUnsigned(df.longCount, writer);
	    PrimitiveWriter.writeIntegerUnsigned(df.bytesCount, writer);
	
	    PrimitiveWriter.writeIntegerUnsigned(df.integerInitCount, writer);
	    int c = df.integerInitCount;
	    while (--c >= 0) {
	        PrimitiveWriter.writeIntegerUnsigned(df.integerInitIndex[c], writer);
	        PrimitiveWriter.writeIntegerSigned(df.integerInitValue[c], writer);
	    }
	
	    PrimitiveWriter.writeIntegerUnsigned(df.longInitCount, writer);
	    c = df.longInitCount;
	    while (--c >= 0) {
	        PrimitiveWriter.writeIntegerUnsigned(df.longInitIndex[c], writer);
	        PrimitiveWriter.writeLongSigned(df.longInitValue[c], writer);
	    }
	
	    PrimitiveWriter.writeIntegerUnsigned(df.byteInitCount, writer);
	    c = df.byteInitCount;
	    while (--c >= 0) {
	        PrimitiveWriter.writeIntegerUnsigned(df.byteInitIndex[c], writer);
	        byte[] value = df.byteInitValue[c];
	        PrimitiveWriter.writeIntegerUnsigned(null==value? -1 :value.length, writer);
	        if (null!=value && value.length>0) {
	        	PrimitiveWriter.writeByteArrayData(value, 0, value.length, writer);
	        }
	    }
	    PrimitiveWriter.writeIntegerUnsigned(df.byteInitTotalLength, writer);
	
	    /*
	     * Fastest searialize deserialize however its more verbose and there is
	     * no object dectection and construction.
	     * 
	     * These files can be deleted and modified but those changes are only
	     * refelected on startup. New templates can be added but an explicit
	     * call must be made to load them. The new templates will be loaded
	     * dynamicaly on first use but this is not recommended.
	     */
	
	}

	public static void writeTemplateCatalog(TemplateHandler handler, int byteGap, int maxByteLength, PrimitiveWriter writer, ClientConfig clientConfig) {
	
	    TemplateHandler.postProcessDictionary(handler, byteGap, maxByteLength);
	
	   //System.err.println("Names:"+ Arrays.toString(catalogScriptFieldNames));
	
	    // write catalog data.
	    save(writer, handler.fieldIdBiggest, handler.templateIdUnique, handler.templateIdBiggest, handler.defaultConstValues,
	    		handler.catalogLargestTemplatePMap, handler.catalogLargestNonTemplatePMap, handler.tokenIdxMembers, handler.tokenIdxMemberHeads,
	    		handler.catalogScriptTokens, handler.catalogScriptFieldIds, handler.catalogScriptFieldNames, handler.catalogScriptDictionaryNames,
	    		handler.catalogTemplateScriptIdx,  handler.templateToOffset, handler.templateToLimit ,
	    		handler.maxGroupTokenStackDepth + 1, clientConfig);
	
	    // close stream.
	    PrimitiveWriter.flush(writer);
	}

	public static int maxPMapCountInBytes(TemplateCatalogConfig catalog) {
        return 2 + ((
                      catalog.maxTemplatePMapSize()>catalog.maxNonTemplatePMapSize() ?
                    		  catalog.maxTemplatePMapSize() + 2:
                    	      catalog.maxNonTemplatePMapSize() + 2) * catalog.getMaxGroupDepth());
    }

}
