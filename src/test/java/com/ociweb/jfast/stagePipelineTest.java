package com.ociweb.jfast;

import static com.ociweb.pronghorn.ring.RingBufferConfig.pipe;
import static com.ociweb.pronghorn.stage.scheduling.GraphManager.getOutputPipe;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.ConsoleStage;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.TestGenerator;
import com.ociweb.pronghorn.stage.test.TestValidator;

public class stagePipelineTest {
    
    private final int seed = 42;
    private final int iterations = 10; //TODO: AA, this seems to generate no data if iteration is 1
    private final long TIMEOUT_SECONDS = 4;

    @Test
    public void roundTripTest() {
        
        String templatePath = "/performance/example.xml";  //TODO: AAA, Does not support the "creative" messages in the simpleExample.xml   
        
        
        FieldReferenceOffsetManager from = buildFROM(templatePath);        
        assertTrue(null!=from);
        
        RingBufferConfig busConfig = new RingBufferConfig(from, 20, 127);
        RingBufferConfig rawConfig = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 20, 1024); 
        
        
        GraphManager gm = new GraphManager();
        
        
   
        PronghornStage generator1 = new TestGenerator(gm, seed, iterations, pipe(busConfig));    
 //       PronghornStage generator2 = new TestGenerator(gm, seed, iterations, pipe(busConfig));            
        //SplitterStage splitter = new SplitterStage(gm, getOutputPipe(gm, generator, 1), pipe(busConfig.grow2x()), pipe(busConfig.grow2x()));        
        
        FASTEncodeStage encoder = new FASTEncodeStage(gm, getOutputPipe(gm, generator1), pipe(rawConfig), templatePath );
     
        new ConsoleStage(gm, getOutputPipe(gm, encoder));
        
        //   FASTDecodeStage decoder = new FASTDecodeStage(gm, getOutputPipe(gm, encoder), pipe(busConfig), templatePath );        
        
     ///   PronghornStage validateResults = new TestValidator(gm, getOutputPipe(gm, generator2), getOutputPipe(gm, decoder));
                
        
        //start the timer       
        final long start = System.currentTimeMillis();
        
   //     GraphManager.enableBatching(gm); //confirm if batching works with FAST
        
        StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));        
        scheduler.startup();        
        
        //blocks until all the submitted runnables have stopped
       
        //this timeout is set very large to support slow machines that may also run this test.
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      
        long duration = System.currentTimeMillis()-start;
        
        
    }
    
    public static FieldReferenceOffsetManager buildFROM(String templatePath) {
        try {
            return TemplateHandler.loadFrom(templatePath);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }   
        return null;
        
    }
}
