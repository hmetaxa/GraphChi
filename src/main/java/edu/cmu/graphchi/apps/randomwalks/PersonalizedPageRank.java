package edu.cmu.graphchi.apps.randomwalks;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.walks.DrunkardContext;
import edu.cmu.graphchi.walks.DrunkardJob;
import edu.cmu.graphchi.walks.DrunkardMobEngine;
import edu.cmu.graphchi.walks.IntDrunkardContext;
import edu.cmu.graphchi.walks.IntDrunkardFactory;
import edu.cmu.graphchi.walks.IntWalkArray;
import edu.cmu.graphchi.walks.WalkUpdateFunction;
import edu.cmu.graphchi.walks.WalkArray;
import edu.cmu.graphchi.walks.WeightedHopper;
import edu.cmu.graphchi.walks.distributions.IntDrunkardCompanion;
import edu.cmu.graphchi.walks.distributions.DrunkardCompanion;
import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;
import org.apache.commons.cli.*;

import java.io.File;
//import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.Naming;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Computes estimate of personalized pagerank using the DrunkardMobEngine.
 * <b>Note:</b> this version omits walks to adjacent vertices, and thus could be
 * a basis for recommendation engine. To remove that functionality, modify
 * method getNotTrackedVertices()
 *
 * @author Aapo Kyrola
 */
public class PersonalizedPageRank implements WalkUpdateFunction<Integer, Float> {

    public enum ExperimentType {

        ACM,
        LFR,
        DBLP
    }

    private static double RESET_PROBABILITY = 0.15;
    private static Logger logger = ChiLogger.getLogger("personalized-pagerank");
    private DrunkardMobEngine<Integer, Float>  drunkardMobEngine;
    private String baseFilename;

    private int numWalksPerSource;
    private String companionUrl;
    private EdgeDirection edgeDirection;

    public PersonalizedPageRank(String companionUrl, String baseFilename, int nShards, int walksPerSource, EdgeDirection edgeDirection) throws Exception {
        this.baseFilename = baseFilename;
        this.drunkardMobEngine = new DrunkardMobEngine<Integer, Float> (baseFilename, nShards,
                new IntDrunkardFactory());
        this.drunkardMobEngine.setEdataConverter(new FloatConverter());
        
        this.companionUrl = companionUrl;

        this.numWalksPerSource = walksPerSource;
        this.edgeDirection = edgeDirection;
    }

    private void execute(int numIters, int[] nodeIds, int nTop) throws Exception {
        //File graphFile = new File(baseFilename);

        /**
         * Use local drunkard mob companion. You can also pass a remote
         * reference by using Naming.lookup("rmi://my-companion")
         */
        RemoteDrunkardCompanion companion = new IntDrunkardCompanion(4, Runtime.getRuntime().maxMemory() / 3);
//        if (companionUrl.equals("local")) {
//            companion = new IntDrunkardCompanion(4, Runtime.getRuntime().maxMemory() / 3);
//        } else {
//            companion = (RemoteDrunkardCompanion) Naming.lookup(companionUrl);
//        }

        /* Configure walk sources. Note, GraphChi's internal ids are used. */
        DrunkardJob drunkardJob = this.drunkardMobEngine.addJob("personalizedPageRank",
                edgeDirection, this, companion);
        //DrunkardJob drunkardJob = this.drunkardMobEngine.addJob("personalizedPageRank",
//                EdgeDirection.OUT_EDGES, this, companion);
        //om int curNumSources = firstSource + numSources > drunkardMobEngine.getEngine().numVertices() ? drunkardMobEngine.getEngine().numVertices() - firstSource : numSources;
        //om drunkardJob.configureSourceRangeInternalIds(firstSource, curNumSources, numWalksPerSource);

        //drunkardJob.configureSourceRangeInternalIds(firstSource, numSources, numWalksPerSource);
        drunkardJob.configureSourceRangeInternalIds(nodeIds, numWalksPerSource);
        drunkardMobEngine.run(numIters);

        /* Ask companion to dump the results to file */
        //int nTop = 60;
        //companion.outputDistributions(baseFilename + "_ppr_" + firstSource + "_"
        //       + (firstSource + numSources - 1) + ".top" + nTop, nTop);

        /* For debug */
        VertexIdTranslate vertexIdTranslate = this.drunkardMobEngine.getVertexIdTranslate();
        //String SQLLitedb = "jdbc:sqlite:C:/projects/Datasets/DBLPManage/citation-network2.db";
        String SQLLitedb = "jdbc:sqlite:" + baseFilename;
        companion.outputDistributions(SQLLitedb, vertexIdTranslate, nTop);

//        IdCount[] topForFirst = companion.getTop(firstSource, 10);
//
//        System.out.println("Top visits from source vertex " + vertexIdTranslate.forward(firstSource) + " (internal id=" + firstSource + ")");
//        for (IdCount idc : topForFirst) {
//            System.out.println(vertexIdTranslate.backward(idc.id) + ": " + idc.count);
//        }
        System.out.println("Results Saved... ");
        /* If local, shutdown the companion */
        if (companion instanceof DrunkardCompanion) {
            ((DrunkardCompanion) companion).close();
        }
        System.out.println("Process finished...");

        //om return curNumSources + firstSource == drunkardMobEngine.getEngine().numVertices() ? -1 : curNumSources + firstSource;
        // }
    }

    /**
     * WalkUpdateFunction interface implementations
     */
//    @Override
//    public void processWalksAtVertex(WalkArray walkArray,
//            ChiVertex<EmptyType, EmptyType> vertex,
//            DrunkardContext drunkardContext_,
//            Random randomGenerator) {
//        int[] walks = ((IntWalkArray) walkArray).getArray();
//        IntDrunkardContext drunkardContext = (IntDrunkardContext) drunkardContext_;
//        int numWalks = walks.length;
//        int numOutEdges = vertex.numOutEdges();
//
//        // Advance each walk to a random out-edge (if any)
//        if (numOutEdges > 0) {
//            for (int i = 0; i < numWalks; i++) {
//                int walk = walks[i];
//
//                // Reset?
//                if (randomGenerator.nextDouble() < RESET_PROBABILITY) {
//                    drunkardContext.resetWalk(walk, false);
//                } else {
//                    int nextHop = vertex.getOutEdgeId(randomGenerator.nextInt(numOutEdges));
//
//                    // Optimization to tell the manager that walks that have just been started
//                    // need not to be tracked.
//                    boolean shouldTrack = true; //!drunkardContext.isWalkStartedFromVertex(walk);
//                    drunkardContext.forwardWalkTo(walk, nextHop, shouldTrack);
//                }
//            }
//
//        } else {
//            // Reset all walks -- no where to go from here
//            for (int i = 0; i < numWalks; i++) {
//                drunkardContext.resetWalk(walks[i], false);
//            }
//        }
//    }
    @Override
    /**
     * Instruct drunkardMob not to track visits to this vertex's immediate
     * out-neighbors.
     */
    public int[] getNotTrackedVertices(ChiVertex<Integer, Float> vertex) {
        int[] notCounted = new int[1];// + vertex.numOutEdges()];
//        for (int i = 0; i < vertex.numOutEdges(); i++) {
//            notCounted[i + 1] = vertex.getOutEdgeId(i);
//        }
        notCounted[0] = vertex.getId();
        return notCounted;
    }

//    /**
//     * WalkUpdateFunction interface implementations
//     */
    
    @Override
    public void processWalksAtVertex(WalkArray walkArray,
            ChiVertex<Integer, Float> vertex,
            DrunkardContext drunkardContext_,
            Random randomGenerator) {
        int[] walks = ((IntWalkArray) walkArray).getArray();
        IntDrunkardContext drunkardContext = (IntDrunkardContext) drunkardContext_;
        int numWalks = walks.length;

        int numEdges = edgeDirection == EdgeDirection.OUT_EDGES
                ? vertex.numOutEdges()
                : vertex.numEdges();
        
        
       
        // Advance each walk to a random out-edge (if any)
        if (numEdges > 0) {
            
            int[] hops = WeightedHopper.generateRandomHopsAliasMethod(randomGenerator, vertex, walks.length,edgeDirection, null);
            
            for (int i = 0; i < numWalks; i++) {
                int walk = walks[i];

                // Reset?
                if (randomGenerator.nextDouble() < RESET_PROBABILITY) {
                    drunkardContext.resetWalk(walk, false);
                } else {
                    
                            int nextHop = edgeDirection == EdgeDirection.OUT_EDGES
                            ? vertex.getOutEdgeId(hops[i])
                            : vertex.edge(hops[i]).getVertexId();
                               
//                            int nextHop = edgeDirection == EdgeDirection.OUT_EDGES
//                            ? vertex.getOutEdgeId(randomGenerator.nextInt(numEdges))
//                            : vertex.edge(randomGenerator.nextInt(numEdges)).getVertexId();

                    // Optimization to tell the manager that walks that have just been started
                    // need not to be tracked.
                    // Omiros: we want to track every node  boolean shouldTrack = !drunkardContext.isWalkStartedFromVertex(walk);
                    boolean shouldTrack = true;
                    //boolean shouldTrack = !drunkardContext.isWalkStartedFromVertex(walk);
                    drunkardContext.forwardWalkTo(walk, nextHop, shouldTrack);
                    
                }
            }

        } else {
            // Reset all walks -- no where to go from here
            for (int i = 0; i < numWalks; i++) {
                drunkardContext.resetWalk(walks[i], false);
            }
        }
    }
//
//    @Override
//    /**
//     * Instruct drunkardMob not to track visits to this vertex's immediate
//     * out-neighbors.
//     */
//    public int[] getNotTrackedVertices(ChiVertex<EmptyType, EmptyType> vertex) {
//        int[] notCounted = new int[1];// + vertex.numOutEdges()];
////        for(int i=0; i < vertex.numOutEdges(); i++) {
////            notCounted[i + 1] = vertex.getOutEdgeId(i);
////        }
//        notCounted[0] = vertex.getId();
//        return notCounted;
//    }

//    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
//        return new FastSharder<Integer, Float>(graphName, numShards, null, null, null, null);
//    }

     protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Float>(graphName, numShards, null, new EdgeProcessor<Float>() {
            public Float receiveEdge(int from, int to, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new IntConverter(), new FloatConverter());
    }
     
    public static void main(String[] args) throws Exception {

        Class.forName("org.sqlite.JDBC");
        /* Configure command line */
        Options cmdLineOptions = new Options();
        cmdLineOptions.addOption("g", "graph", true, "graph file name");
        cmdLineOptions.addOption("n", "nshards", true, "number of shards");
        cmdLineOptions.addOption("t", "filetype", true, "filetype (edgelist|adjlist)");
        cmdLineOptions.addOption("f", "firstsource", true, "id of the first source vertex (internal id)");
        cmdLineOptions.addOption("s", "nsources", true, "number of sources");
        cmdLineOptions.addOption("w", "walkspersource", true, "number of walks to start from each source");
        cmdLineOptions.addOption("i", "niters", true, "number of iterations");
        cmdLineOptions.addOption("u", "companion", true, "RMI url to the DrunkardCompanion or 'local' (default)");

        try {

            /* Parse command line */
            CommandLineParser parser = new PosixParser();
            CommandLine cmdLine = parser.parse(cmdLineOptions, args);

            /**
             * Preprocess graph if needed
             */
            //String SQLLitedb = "jdbc:sqlite:C:/projects/Datasets/ACM/PTM3DB.db";
            int nShards = 1;
            int walksPerSource = 1000;//Integer.parseInt(cmdLine.getOptionValue("walkspersource"));
            int nIters = 3;//Integer.parseInt(cmdLine.getOptionValue("niters"));
            int nTop = 100; //200;//300;
            EdgeDirection edgeDirection = EdgeDirection.IN_AND_OUT_EDGES;

            ExperimentType experimentType = ExperimentType.ACM;
            boolean ubuntu = false;
            String dbFilename = "";
            String dictDir = "";

            if (experimentType == ExperimentType.ACM) {
                dbFilename = "PTMDB_ACM2016.db";
                if (ubuntu) {
                    dictDir = ":/home/omiros/Projects/Datasets/ACM/";
                } else {
                    dictDir = "C:\\projects\\Datasets\\ACM\\";
                }
            } else if (experimentType == ExperimentType.LFR) {
                dbFilename = "LFRNet.db";
                if (ubuntu) {
                    dictDir = ":/home/omiros/Projects/Datasets/OverlappingNets/";
                } else {
                    dictDir = "C:\\Projects\\datasets\\OverlappingNets\\LFR\\100K\\NoNoise\\";
                }
            }
            else if (experimentType == ExperimentType.DBLP) {
            dbFilename = "DBLP4Areas.db";
            if (ubuntu) {
                dictDir = ":/home/omiros/Projects/Datasets/DBLP/";
            } else {
                dictDir = "C:\\Projects\\datasets\\DBLP\\";
            }
        }

            String baseFilename = dictDir + dbFilename; //"C:/projects/Datasets/ACM/PTMDB_ACM2016.db"; //cmdLine.getOptionValue("graph");
            String SQLLitedb = "jdbc:sqlite:" + baseFilename;

            String selectSql = "";
            String selectPubsSql = "";

            if (experimentType == ExperimentType.ACM) {
                selectSql = "select source.RowId  as Node1,PubCitation.PubId,  target.RowId  as Node2, PubCitation.CitationId, 1 AS Value\n"
                        + "FROM PubCitation \n"
                        + "INNER JOIN PubCitationPPRAlias source on source.OrigId = PubCitation.pubId\n"
                        + "INNER JOIN PubCitationPPRAlias target on target.OrigId = PubCitation.CitationId \n"
                        + "and PubCitation.citationId in\n"
                        + "(select  PubCitation.citationId from pubcitation group by citationId having count(*)>4) ";

                selectPubsSql = "select distinct source.RowId  as NodeId \n"
                        + "                    FROM PubCitation \n"
                        + "                    INNER JOIN PubCitationPPRAlias source on source.OrigId = PubCitation.pubId  \n"
                        + "         order by source.RowId";

            } else if (experimentType == ExperimentType.LFR) {
                selectSql = "select  Node1 , Node2, 1 As Value\n"
                        + "From Links ";

                selectPubsSql = "select nodeId FROM\n"
                        + "(\n"
                        + "select distinct Node1 as NodeId From Links\n"
                        + "UNION \n"
                        + "select distinct Node2 as NodeId From Links\n"
                        + ") \n"
                        + "Order By NodeId";
            }
            else if (experimentType == ExperimentType.DBLP) {
                
                 selectSql = "select source.RowId  as Node1, target.RowId  as Node2, count AS Value\n"
                        + "FROM coAuthor \n"
                        + "INNER JOIN AuthorPPRAlias source on source.OrigId = coAuthor.Author1\n"
                        + "INNER JOIN AuthorPPRAlias target on target.OrigId = coAuthor.Author2 \n"
                        ;

                selectPubsSql = "select distinct source.RowId  as NodeId \n"
                        + "                    FROM CoAuthorTwoWay \n"
                        + "                    INNER JOIN AuthorPPRAlias source on source.OrigId = CoAuthorTwoWay.Author1  \n"
                        + "         order by source.RowId";
                
                
            }

            //String fileType = (cmdLine.hasOption("filetype") ? cmdLine.getOptionValue("filetype") : null);
            //--graph=C:/projects/Datasets/DBLPManage/citation-network2_NET.csv --nshards=4 --niters=5 --firstsource=0 --walkspersource=1000 --nsources=1000
            /* Create shards */
            FastSharder sharder = createSharder(baseFilename, nShards);

            if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
                sharder.shard(SQLLitedb, selectSql);
            } else {
                logger.info("Found shards -- no need to pre-process");
            }

            // Run
            String companionUrl = cmdLine.hasOption("companion") ? cmdLine.getOptionValue("companion") : "local";
            Connection connection = null;
            int[] nodeIds = null;

            try {

                connection = DriverManager.getConnection(SQLLitedb);

                Statement statement = connection.createStatement();
                statement.setQueryTimeout(60);  // set timeout to 30 sec.

                statement.executeUpdate("create table if not exists PPRLinks (Source int, Target int, Counts int)");
                String deleteSQL = String.format("Delete from PPRLinks  ");
                statement.executeUpdate(deleteSQL);

                ResultSet rsCnt = statement.executeQuery(" select count(nodeId) as Cnt FROM (" + selectPubsSql + " )");
                if (rsCnt.next()) {
                    nodeIds = new int[rsCnt.getInt("Cnt")];
                }

                ResultSet rs = statement.executeQuery(selectPubsSql);
                int i = 0;
                while (rs.next()) {
                    nodeIds[i++] = rs.getInt("NodeId");

                }

            } catch (SQLException e) {
                // if the error message is "out of memory", 
                // it probably means no database file is found
                System.err.println(e.getMessage());
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    // connection close failed.
                    System.err.println(e);
                }
            }

            PersonalizedPageRank pp = new PersonalizedPageRank(companionUrl, baseFilename, nShards, walksPerSource, edgeDirection);
            pp.execute(nIters, nodeIds, nTop);

            logger.info("PPR executed !!!");
//            for (int j = 0; j < nodeIds.length; j++) {
//                firstSource = nodeIds[j];
//                PersonalizedPageRank pp = new PersonalizedPageRank(companionUrl, baseFilename, nShards,
//                        firstSource, 1, walksPerSource, edgeDirection);
//                pp.execute(nIters);
//            }
        } catch (Exception err) {
            err.printStackTrace();
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("PersonalizedPageRank", cmdLineOptions);
        }
    }
}
