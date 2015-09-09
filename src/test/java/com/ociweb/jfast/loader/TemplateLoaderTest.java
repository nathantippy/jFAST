package com.ociweb.jfast.loader;

import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArrayEquals;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeBundle;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.schema.loader.DictionaryFactory;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.Histogram;
import com.ociweb.pronghorn.pipe.util.LocalHeap;


public class TemplateLoaderTest {



    @Test
    public void buildRawCatalog() {

        byte[] catalogByteArray = buildRawCatalogData(new ClientConfig());
        assertEquals(760, catalogByteArray.length);


        // reconstruct Catalog object from stream
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catalogByteArray);

        boolean ok = false;
        int[] script = null;
        try {
            // /performance/example.xml contains 3 templates.
            assertEquals(3, catalog.templatesCount());

            script = catalog.fullScript();
            assertEquals(54, script.length);
            assertEquals(TypeMask.Group, TokenBuilder.extractType(script[0]));// First
                                                                                  // Token

            // CMD:Group:010000/Close:PMap::010001/9
            assertEquals(TypeMask.Group, TokenBuilder.extractType(script[script.length - 1]));// Last
                                                                                              // Token

            ok = true;
        } finally {
            if (!ok) {
                System.err.println("Script Details:");
                if (null != script) {
                    System.err.println(convertScriptToString(script));
                }
            }
        }
    }

    private String convertScriptToString(int[] script) {
        StringBuilder builder = new StringBuilder();
        for (int token : script) {

            builder.append(TokenBuilder.tokenToString(token));

            builder.append("\n");
        }
        return builder.toString();
    }


    @Test
    public void testDecodeComplex30000Minimal() {


        byte[] catBytes = buildRawCatalogData(new ClientConfig());
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));

        System.err.println(sourceDataFile.getName()+ " "+sourceDataFile.length());

        FASTInput fastInput = null;
		try {
			FileInputStream fist = new FileInputStream(sourceDataFile);
			fastInput = new FASTInputStream(fist);
		} catch (FileNotFoundException e) {

			throw new RuntimeException(e);
		}

        FASTClassLoader.deleteFiles();
        final AtomicInteger msgs = new AtomicInteger();

        FASTReaderReactor reactor = FAST.inputReactor(fastInput, catBytes, PipeBundle.buildRingBuffers(new Pipe(new PipeConfig((byte)15, (byte)15, catalog.ringByteConstants(), catalog.getFROM())).initBuffers())); 
        
        assertEquals(1,reactor.ringBuffers().length);
        Pipe rb = reactor.ringBuffers()[0];
        rb.reset();

        while (FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
            if (PipeReader.tryReadFragment(rb)) {	
	            if (PipeReader.isNewMessage(rb)) {
	                int templateId = PipeReader.getMsgIdx(rb);
	                if (templateId<0) {
	                	break;
	                }
	                msgs.incrementAndGet();
	            }
                PipeReader.releaseReadLock(rb);
            }
        }
        System.out.println("total messages:"+msgs);


    }

    @Test
    public void testDecodeEncodeComplex30000() {

        FASTClassLoader.deleteFiles();

        byte[] catBytes = buildRawCatalogData(new ClientConfig());
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();
        final byte[] testBytesData = buildInputArrayForTesting(sourceDataFile);

        FASTInputByteArray fastInput = new FASTInputByteArray(testBytesData);

        // New memory mapped solution. No need to cache because we warm up and
        // OS already has it.
        // FASTInputByteBuffer fastInput =
        // buildInputForTestingByteBuffer(sourceDataFile);

        PrimitiveReader reader = new PrimitiveReader(2048, fastInput, maxPMapCountInBytes);
		ClientConfig r = catalog.clientConfig();

        FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes, PipeBundle.buildRingBuffers(new Pipe(new PipeConfig((byte)15, (byte)15, catalog.ringByteConstants(), catalog.getFROM())).initBuffers()));

       // readerDispatch = new FASTReaderInterpreterDispatch(catBytes);//not using compiled code

       System.err.println("using: "+readerDispatch.getClass().getSimpleName());

        final AtomicInteger msgs = new AtomicInteger();

        FASTReaderReactor reactor = new FASTReaderReactor(readerDispatch,reader);

        Pipe queue = PipeBundle.get(readerDispatch.ringBuffers,0);

        FASTOutputByteArrayEquals fastOutput = new FASTOutputByteArrayEquals(testBytesData,Pipe.from(queue).tokens);

        int writeBuffer = 256;
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, false);

        //unusual case just for checking performance. Normally one could not pass the catalog.ringBuffer() in like this.
        //FASTEncoder writerDispatch = new FASTWriterInterpreterDispatch(catalog, readerDispatch.ringBuffers);
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes);
        //System.err.println("using: "+writerDispatch.getClass().getSimpleName());

        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, queue, writerDispatch);

        System.gc();

        int warmup = 20;// set much larger for profiler
        int count = 128;


        long wroteSize = 0;
        msgs.set(0);
        int grps = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs.set(0);
            grps = 0;
            DictionaryFactory dictionaryFactory = writerDispatch.dictionaryFactory;
            
            dictionaryFactory.reset(writerDispatch.rIntDictionary);
            dictionaryFactory.reset(writerDispatch.rLongDictionary);
            LocalHeap.reset(writerDispatch.byteHeap);
            
            while (FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or a fragment is read

                    if (PipeReader.tryReadFragment(queue)) {
                        if (PipeReader.isNewMessage(queue)) {
                            msgs.incrementAndGet();   	
                        }
                        try{   
                            FASTDynamicWriter.write(dynamicWriter);
                        } catch (FASTException e) {
                            System.err.println("ERROR: cursor at "+writerDispatch.getActiveScriptCursor()+" "+TokenBuilder.tokenToString(Pipe.from(queue).tokens[writerDispatch.getActiveScriptCursor()]));
                            throw e;
                        }    
                       
                        grps++;
                        PipeReader.releaseReadLock(queue);
                    }
            }            

            queue.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            FASTDecoder.reset(catalog.dictionaryFactory(), readerDispatch);

            PrimitiveWriter.flush(writer);
            wroteSize = Math.max(wroteSize, PrimitiveWriter.totalWritten(writer));
            fastOutput.reset();
            PrimitiveWriter.reset(writer);
            dynamicWriter.reset(true);

        }

        // Expected total read fields:2126101
        assertEquals("test file bytes", totalTestBytes, wroteSize);

        iter = count;
        while (--iter >= 0) {

            DictionaryFactory dictionaryFactory = writerDispatch.dictionaryFactory;
            dictionaryFactory.reset(writerDispatch.rIntDictionary);
            dictionaryFactory.reset(writerDispatch.rLongDictionary);
            LocalHeap.reset(writerDispatch.byteHeap);
            double start = System.nanoTime();
            
            while (FASTReaderReactor.pump(reactor)>=0) {  
                    if (PipeReader.tryReadFragment(queue)) {
                       if (PipeReader.getMsgIdx(queue)>=0) { //skip if we are waiting for more content.
                                FASTDynamicWriter.write(dynamicWriter);  
                             //   RingBuffer.releaseReadLock(queue);
                       }
                       PipeReader.releaseReadLock(queue);
                    }
            }
            
            
            double duration = System.nanoTime() - start;

            if ((0x3F & iter) == 0) {
                int ns = (int) duration;
                float mmsgPerSec = (msgs.intValue() * (float) 1000l / ns);
                float nsPerByte = (ns / (float) totalTestBytes);
                int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

                System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                        + " " + mbps + "mbps " + " Bytes:" + totalTestBytes + " Messages:" + msgs + " Groups:" + grps); // Phrases/Clauses
            }

            // //////
            // reset the data to run the test again.
            // //////
            queue.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            FASTDecoder.reset(catalog.dictionaryFactory(), readerDispatch);

            fastOutput.reset();
            PrimitiveWriter.reset(writer);
            dynamicWriter.reset(true);

        }

    }


    @Test
    public void testEncodeComplex30000() {

        FASTClassLoader.deleteFiles();

        byte[] catBytes = buildRawCatalogData(new ClientConfig());
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        int maxPMapCountInBytes = TemplateCatalogConfig.maxPMapCountInBytes(catalog);

        // connect to file
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
        long totalTestBytes = sourceDataFile.length();
        final byte[] testBytesData = buildInputArrayForTesting(sourceDataFile);

        FASTInputByteArray fastInput = new FASTInputByteArray(testBytesData);

        final AtomicInteger msgs = new AtomicInteger();

        PrimitiveReader reader = new PrimitiveReader(4096, fastInput, maxPMapCountInBytes);

        FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes, PipeBundle.buildRingBuffers(new Pipe(new PipeConfig((byte)22, (byte)20, catalog.ringByteConstants(), catalog.getFROM())).initBuffers()));
        FASTReaderReactor reactor = new FASTReaderReactor(readerDispatch,reader);

        System.err.println("usingReader: "+readerDispatch.getClass().getSimpleName());

        Pipe dataToEncodePipe = PipeBundle.get(readerDispatch.ringBuffers,0);

        FASTOutputByteArrayEquals fastOutput = new FASTOutputByteArrayEquals(testBytesData,Pipe.from(dataToEncodePipe).tokens);

        int writeBuffer = 16384;
        boolean minimizeLatency = false;
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput, minimizeLatency);

        //unusual case just for checking performance. Normally one could not pass the catalog.ringBuffer() in like this.
         //FASTEncoder writerDispatch = new FASTWriterInterpreterDispatch(catalog, readerDispatch.ringBuffers);
         FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes);

        System.err.println("usingWriter: "+writerDispatch.getClass().getSimpleName());

        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, dataToEncodePipe, writerDispatch);

        int warmup = 10;// set much larger for profiler
        int count = 128;


        long wroteSize = 0;
        msgs.set(0);
        int grps = 0;
        int iter = warmup;
        while (--iter >= 0) {
            msgs.set(0);
            grps = 0;
            DictionaryFactory dictionaryFactory = writerDispatch.dictionaryFactory;
            
            dictionaryFactory.reset(writerDispatch.rIntDictionary);
            dictionaryFactory.reset(writerDispatch.rLongDictionary);
            LocalHeap.reset(writerDispatch.byteHeap);

            
            //read from reader and puts messages on the queue
            while (FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or a fragment is read

            	assert(Pipe.bytesWorkingTailPosition(dataToEncodePipe)<=Pipe.bytesHeadPosition(dataToEncodePipe));
            	
            	    //////////////////////////////////////////////
            		//confirms full fragment to read on the queue            	
                    if (PipeReader.tryReadFragment(dataToEncodePipe)) {
                    ///////////////////////////////////////////    
                        if (PipeReader.isNewMessage(dataToEncodePipe)) {
                        	int msgIdx = PipeReader.getMsgIdx(dataToEncodePipe);
							if (msgIdx<0) {
                        		break;
                        	}
                            msgs.incrementAndGet();
                        }
                        assert(Pipe.bytesWorkingTailPosition(dataToEncodePipe)<=Pipe.bytesHeadPosition(dataToEncodePipe));
                        try{
                            ///////////////////////////////////////////////////////
                        	//write fragment found on the queue to the output writer
                        	FASTDynamicWriter.write(dynamicWriter);
                        	////////////////////////////////////////////////////
                        } catch (FASTException e) {
                            System.err.println("ERROR: cursor at "+writerDispatch.getActiveScriptCursor()+" "+TokenBuilder.tokenToString(Pipe.from(dataToEncodePipe).tokens[writerDispatch.getActiveScriptCursor()]));
                            throw e;
                        }
                        grps++;
                        //////////////////////////////////////
                        PipeReader.releaseReadLock(dataToEncodePipe);
                        ///////////////////////////////////
                    }

            }


            dataToEncodePipe.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            FASTDecoder.reset(catalog.dictionaryFactory(), readerDispatch);

            PrimitiveWriter.flush(writer);
            wroteSize = Math.max(wroteSize, PrimitiveWriter.totalWritten(writer));
            fastOutput.reset();
            PrimitiveWriter.reset(writer);
            dynamicWriter.reset(true);

        }

        // Expected total read fields:2126101
        assertEquals("test file bytes at msg "+msgs.get(), totalTestBytes, wroteSize);

        //In the warm up we checked the writes for accuracy, here we are only going for speed
        //so the FASTOutput instance is changed to one that only writes.
        FASTOutputByteArray fastOutput2 = new FASTOutputByteArray(testBytesData);
 //       FASTOutput fastOutput2 = new FASTOutputTotals();

        writer = new PrimitiveWriter(writeBuffer, fastOutput2, minimizeLatency);
        dynamicWriter = new FASTDynamicWriter(writer, dataToEncodePipe, writerDispatch);

        boolean concurrent = false; //when set true this is not realistic use case but it is a nice test point.

        iter = count;
        while (--iter >= 0) {

            DictionaryFactory dictionaryFactory = writerDispatch.dictionaryFactory;
            dictionaryFactory.reset(writerDispatch.rIntDictionary);
            dictionaryFactory.reset(writerDispatch.rLongDictionary);
            LocalHeap.reset(writerDispatch.byteHeap);

            final ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);

            //note this test is never something that represents a normal use case but it is good for testing the encoding only time.

            //Pre-populate the ring buffer to only measure the write time.
            final AtomicBoolean isAlive = reactor.start(executor, reader);
            double start;

            if (concurrent) {
                start = System.nanoTime();
                while (isAlive.get()) {
                    while (PipeReader.tryReadFragment(dataToEncodePipe)) {
                        FASTDynamicWriter.write(dynamicWriter);
                        PipeReader.releaseReadLock(dataToEncodePipe);
                    }
                }
                while (PipeReader.tryReadFragment(dataToEncodePipe)) {
                    FASTDynamicWriter.write(dynamicWriter);
                    PipeReader.releaseReadLock(dataToEncodePipe);
                }

            } else {
                //wait until everything is decoded

                while (isAlive.get()) {
                }
                //now start the timer
                start = System.nanoTime();

                while (PipeReader.tryReadFragment(dataToEncodePipe)) {
                        FASTDynamicWriter.write(dynamicWriter);
                        PipeReader.releaseReadLock(dataToEncodePipe);
                }
            }
            double duration = System.nanoTime() - start;
            // Only shut down after is alive is finished.
            executor.shutdown();

            try {
                executor.awaitTermination(1,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();

            }


            if ((0x3F & iter) == 0) {
                int ns = (int) duration;
                float mmsgPerSec = (msgs.intValue() * (float) 1000l / ns);
                float nsPerByte = (ns / (float) totalTestBytes);
                int mbps = (int) ((1000l * totalTestBytes * 8l) / ns);

                System.err.println("Duration:" + ns + "ns " + " " + mmsgPerSec + "MM/s " + " " + nsPerByte + "nspB "
                        + " " + mbps + "mbps " + " Bytes:" + totalTestBytes + " Messages:" + msgs + " Groups:" + grps +" "+(concurrent?"concurrent":"sequential")); // Phrases/Clauses
            }

            // //////
            // reset the data to run the test again.
            // //////
            dataToEncodePipe.reset();

            fastInput.reset();
            PrimitiveReader.reset(reader);
            FASTDecoder.reset(catalog.dictionaryFactory(), readerDispatch);

            fastOutput2.reset();

            PrimitiveWriter.reset(writer);
            dynamicWriter.reset(true);

        }

    }

	static byte[] buildInputArrayForTesting(File fileSource) {
        byte[] fileData = null;
        try {
            // do not want to time file access so copy file to memory
            fileData = new byte[(int) fileSource.length()];
            FileInputStream inputStream = new FileInputStream(fileSource);
            int readBytes = inputStream.read(fileData);
            inputStream.close();
            assertEquals(fileData.length, readBytes);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileData;
    }


    @Ignore
    public void testDecodeTemplateRefMinimal() {


        byte[] catBytes = buildRawCatalogData(new ClientConfig(), 0, "/template/templateRefExample.xml");
        final TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);

        final int MSG_TEST_LOC = lookupTemplateLocator("Test",catalog.getFROM());


        int field1locator = lookupFieldLocator("field1", MSG_TEST_LOC, catalog.getFROM());
        int field2locator = lookupFieldLocator("field2", MSG_TEST_LOC, catalog.getFROM());
        int field3locator = lookupFieldLocator("field3", MSG_TEST_LOC, catalog.getFROM());

        System.err.println("cat bytes:"+catBytes.length);

        FASTInput fastInput = null;
        byte[] rawbytes={(byte) 0xf8, (byte) 0x82, (byte) 0x81, (byte) 0x82, (byte) 0x83};
		ByteArrayInputStream fist = new ByteArrayInputStream( rawbytes );
		fastInput = new FASTInputStream(fist);


        FASTClassLoader.deleteFiles();
        final AtomicInteger msgs = new AtomicInteger();
		ClientConfig r = catalog.clientConfig();

        FASTReaderReactor reactor = FAST.inputReactor(fastInput, catBytes, PipeBundle.buildRingBuffers(new Pipe(new PipeConfig((byte)15, (byte)15, catalog.ringByteConstants(), catalog.getFROM())).initBuffers()));

        assertEquals(1,reactor.ringBuffers().length);
        Pipe rb = reactor.ringBuffers()[0];
        rb.reset();

        while (FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
            if (PipeReader.tryReadFragment(rb)) {
	            if (PipeReader.isNewMessage(rb)) {
	                int templateId = PipeReader.getMsgIdx(rb);
	                if (templateId<0) {
	                	break;
	                }
                    assertEquals(1, PipeReader.readInt(rb, field1locator));
                    assertEquals(2, PipeReader.readInt(rb, field2locator));
                    assertEquals(3, PipeReader.readInt(rb, field3locator));
	                msgs.incrementAndGet();
	            }
	            PipeReader.releaseReadLock(rb);
            }
        }
        System.out.println("total messages:"+msgs);
    }


    public static byte[] buildRawCatalogData(ClientConfig clientConfig, int preemble, String file) {
        //this example uses the preamble feature
        clientConfig.setPreableBytes((short)preemble);

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, file, clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }

    public static byte[] buildRawCatalogData(ClientConfig clientConfig) {
        return buildRawCatalogData(clientConfig, 4, "/performance/example.xml");
    }

}
