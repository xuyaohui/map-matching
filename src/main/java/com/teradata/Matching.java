package com.teradata;

import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.matching.MapMatching;
import com.graphhopper.matching.MatchResult;
import com.graphhopper.matching.gpx.Gpx;
import com.graphhopper.matching.gpx.Trk;
import com.graphhopper.matching.gpx.Trkpt;
import com.graphhopper.matching.gpx.Trkseg;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.routing.util.HintsMap;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.util.*;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @program : mapmatchingnew
 * @description : 路网拟合
 * @author: xuyaohui
 * @create: 2019-09-17-14
 */
public class Matching extends UDF {

    public Text evaluate(Text str) {

        String retStr = "";


        GraphHopper hopper = new GraphHopperOSM().forServer();
//        hopper.getCHFactoryDecorator().setEnabled(false);
//        hopper.getCHFactoryDecorator().setWeightingsAsStrings("fastest|car");
        hopper.setGraphHopperLocation("graph-cache/");
//        hopper.setEncodingManager(new EncodingManager("car,bike,foot"));

        // now this can take minutes if it imports or a few seconds for loading
        // of course this is dependent on the area you import
        hopper.getCHFactoryDecorator().setEnabled(false);

        hopper.importOrLoad();


//        FlagEncoder firstEncoder = hopper.getEncodingManager().fetchEdgeEncoders().get(0);
        AlgorithmOptions opts = AlgorithmOptions.start().
                algorithm(Parameters.Algorithms.ASTAR_BI).traversalMode(hopper.getTraversalMode()).
//                weighting(new FastestWeighting(firstEncoder)).
                maxVisitedNodes(1000).
                hints(new HintsMap().put("weighting", "shortest").put("vehicle", "car").put("ch.disable",true).put("ch",false)).
                build();
        MapMatching mapMatching = new MapMatching(hopper, opts);
        mapMatching.setTransitionProbabilityBeta(2.0);
        mapMatching.setMeasurementErrorSigma(50);

        StopWatch importSW = new StopWatch();
        StopWatch matchSW = new StopWatch();

        Translation tr = new TranslationMap().doImport().getWithFallBack(Helper.getLocale(""));

        //MatchResult mr = null;
        //根据字符串创建GPX对象
        Gpx gpx = this.getGpxFromStr(str.toString());
        //Gpx gpx = this.getGpxFromStr("0.0,30.351741,120.055171,2018-10-29 08:11:22|0.0,30.341572,120.054932,2018-10-29 08:22:50|0.0,30.354972,120.057352,2018-10-29 08:23:01|0.0,30.351740,120.055170,2018-10-29 08:23:49|0.0,30.351741,120.055171,2018-10-29 08:23:54|0.0,30.341455,120.061115,2018-10-29 08:24:25|0.0,30.341485,120.061122,2018-10-29 08:24:27|0.0,30.341455,120.061115,2018-10-29 08:28:51|0.0,30.341485,120.061122,2018-10-29 08:28:57|0.0,30.340802,120.062503,2018-10-29 08:29:23|0.0,30.340802,120.062503,2018-10-29 08:29:33|0.0,30.342711,120.061213,2018-10-29 08:34:37|0.0,30.331925,120.068621,2018-10-29 08:36:24|0.0,30.330229,120.067565,2018-10-29 08:37:31|0.0,30.329510,120.063480,2018-10-29 08:37:35|0.0,30.331925,120.068621,2018-10-29 08:37:36|0.0,30.329510,120.063480,2018-10-29 08:37:39|0.0,30.331925,120.068621,2018-10-29 08:37:40|0.0,30.329510,120.063480,2018-10-29 08:37:45|0.0,30.331925,120.068621,2018-10-29 08:37:46|0.0,30.329510,120.063480,2018-10-29 08:37:47|0.0,30.318014,120.056751,2018-10-29 08:40:46|0.0,30.324280,120.062250,2018-10-29 08:42:44|0.0,30.310420,120.062550,2018-10-29 08:42:53|0.0,30.321571,120.064941,2018-10-29 08:45:01|0.0,30.318070,120.056470,2018-10-29 08:46:15|0.0,30.318014,120.056751,2018-10-29 08:57:49|");
        //地图匹配
        //mr = mapMatching.doWork(gpx.trk.get(0).getEntries());
        //mr.getEdgeMatches();
        List<GPXEntry> measurements = gpx.trk.get(0).getEntries();
        importSW.stop();
        matchSW.start();
        MatchResult mr = mapMatching.doWork(measurements);
        matchSW.stop();
        PathWrapper pathWrapper = new PathWrapper();
        new PathMerger().doWork(pathWrapper, Collections.singletonList(mr.getMergedPath()), tr);
        InstructionList instructionList = pathWrapper.getInstructions();
        List<GPXEntry> gpxEntryList = instructionList.createGPXList();
        //拿到第一个node的开始时间
        long time = System.currentTimeMillis();
        if (!measurements.isEmpty()) {
            time = measurements.get(0).getTime();
        }
        for(int i=0;i<gpxEntryList.size();i++){
            GPXEntry gpxEntry = gpxEntryList.get(i);
            String rowStr=i+","+gpxEntry.getLat()+","+gpxEntry.getLon()+","+timeStamp2Date(gpxEntry.getTime()+time,"yyyy-MM-dd HH:mm:ss");
            retStr=retStr+rowStr+"|";
        }

        return new Text(retStr);
    }

    public static String timeStamp2Date(Long seconds,String format) {
        if(seconds == null || seconds.longValue()==0L){
            return "";
        }
        if(format == null || format.equalsIgnoreCase("")){
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(seconds));
    }

    public Gpx getGpxFromStr(String str){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (str==null) return null;
        if ("".equals(str)) return null;
        Gpx gpx = new Gpx();
        Trk trk = new Trk();
        Trkseg trkseg = new Trkseg();
        List<Trkpt> trkptList = new ArrayList<Trkpt>();
        String[] point = str.split("\\|");//|分割后，取得每个Trkpt的字符串
        for (int i=0;i<point.length;i++){
            String pointStr = point[i];
            String[] element = pointStr.split(",");
            Trkpt trkpt = new Trkpt();
            String latStr = element[1];
            String lonStr = element[2];
            String dateStr = element[3];
            //类型转换
            trkpt.ele=0.0;
            try {
                Double lat = Double.parseDouble(latStr);
                Double lon= Double.parseDouble(lonStr);
                trkpt.lat=lat;
                trkpt.lon=lon;
            } catch (NumberFormatException e) {
                //e.printStackTrace();
            }
            try {
                trkpt.time=formatter.parse(dateStr);
            } catch (ParseException e) {
                //e.printStackTrace();
            }
            trkptList.add(trkpt);
        }
        trkseg.trkpt=trkptList;
        trk.name="GraphHopper Track";
        List<Trkseg> trksegList = new ArrayList<Trkseg>();
        trksegList.add(trkseg);
        trk.trkseg=trksegList;
        List<Trk> trkList = new ArrayList<Trk>();
        trkList.add(trk);
        gpx.trk=trkList;
        return gpx;
    }

    public static void main(String[] args) throws IOException {
        Matching matching=new Matching();
//        String str="0.0,30.351741,120.055171,2018-10-29 08:11:22|0.0,30.341572,120.054932,2018-10-29 08:22:50|0.0,30.354972,120.057352,2018-10-29 08:23:01|0.0,30.351740,120.055170,2018-10-29 08:23:49|0.0,30.351741,120.055171,2018-10-29 08:23:54|0.0,30.341455,120.061115,2018-10-29 08:24:25|0.0,30.341485,120.061122,2018-10-29 08:24:27|0.0,30.341455,120.061115,2018-10-29 08:28:51|0.0,30.341485,120.061122,2018-10-29 08:28:57|";
        String str ="0,30.351741,120.055171,2018-10-29 08:11:22|0,30.341572,120.054932,2018-10-29 08:11:22|0,30.341455,120.061115,2018-10-29 08:11:22|0,30.342711,120.061213,2018-10-29 08:11:22|0,30.331925,120.068621,2018-10-29 08:11:22|0,30.330229,120.067565,2018-10-29T08:37:31+00:00|0,30.318014,120.056751,2018-10-29T08:40:46+00:00|";
        Text text = matching.evaluate(new Text(str));
        System.out.print(text.toString());
    }
}
