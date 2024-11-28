/* P4_16 */
#include <core.p4> // Mandatory for all programs.
#include <v1model.p4>


typedef bit<16> egressSpec_t;
typedef bit<16> ingressp; //Size of egress spec in standard_metadata
typedef bit<16> hashp;
typedef bit<32> teid;
register <bit<16>>(1) myh;
register <bit<48>>(1024) myh1;
register <bit<48>>(1024) myh2;
register <bit<16>>(1) myh3;
#define BASE_VF_PORT 768
typedef bit<48> macp;


typedef bit<32> teidf;

//typedef bit<38> teidqfif;
typedef bit<3> rbnumber;
typedef bit<1> rqibool;

#define CPU_CLONE_SESSION_ID 99

// standard_metadata.instance_type

// 0x0 - normal packet
// 0x1 - clone ingress to ingress
// 0x2 - clone egress to ingress
// 0x3 - recirculate packet
// 0x4 - resubmit packet
// 0x8 - clone ingress to egress
// 0x9 - clone egress to egress
// 0xa - multicast packet

// standard_metadata.clone_spec
// Clone specification ID is a simple identifier which will be
// set for the entire cloning operation. It is not used internally
// in all cases but ingress to egress cloning. In this case, if bit
// 31 is set, the least significant bits will be interpreted as
// egress_spec, thus allowing the cloned packet forward to be
// controlled

const bit<4> BMV2_V1MODEL_INSTANCE_TYPE_NORMAL        = 0;
const bit<4> BMV2_V1MODEL_INSTANCE_TYPE_INGRESS_CLONE = 1;
const bit<4> NETRO_INSTANCE_TYPE_I2E_CLONE      = 0x8;
const bit<4> NETRO_INSTANCE_TYPE_I2I_CLONE      = 0x1;
const bit<4> NETRO_INSTANCE_TYPE_E2E_CLONE      = 0x9;

const bit<4> NETRO_NO_CLONE = 0;
const bit<4> NETRO_I2E = 1;

// #define IS_I2E_CLONE(std_meta) (std_meta.instance_type == BMV2_V1MODEL_INSTANCE_TYPE_INGRESS_CLONE)
// #define IS_I2E_CLONE_NETRO(std_meta) (std_meta.instance_type == NETRO_INSTANCE_TYPE_I2E_CLONE)
const bit<32> I2E_CLONE_SESSION_ID = 5;


/** Headers **/
/* Ethernet header definition */
header eth_h {
        bit<48> dmac;
        bit<48> smac;
        bit<16> type;
}

header ipv4_h {
        bit<4>  version;
        bit<4>  ihl;
        bit<8>  diffserv;
        bit<16> totalLen;
        bit<16> identification;
        bit<3>  flags;
        bit<13> fragOffset;
        bit<8>  ttl;
        bit<8>  protocol;
        bit<16> hdrChecksum;
        bit<32> srcAddr;
        bit<32> dstAddr;
}

header udp_h {
        bit<16> srcPort;
        bit<16> dstPort;
        bit<16> len;
        bit<16> checksum;
}

header inneripv4_h {
        bit<4>  version;
        bit<4>  ihl;
        bit<8>  diffserv;
        bit<16> totalLen;
        bit<16> identification;
        bit<3>  flags;
        bit<13> fragOffset;
        bit<8>  ttl;
        bit<8>  protocol;
        bit<16> hdrChecksum;
        bit<32> srcAddr;
        bit<32> dstAddr;
}

header innerudp_h {
        bit<16> srcPort;
        bit<16> dstPort;
        bit<16> len;
        bit<16> checksum;
}

header gtpu_h {
        bit<8>  flags;
        bit<8>  type;
        bit<16> length;
        bit<32> teid;
        bit<16> seq_num;
        bit<8>  qfi;
}



header physical_buffer_h {
        bit<8>  nack_count;
        bit<16>  pf_number;
        bit<32> endpoint_id; 
        bit<16> ack_sn;
}

header rlc_ack_mode_h {
        bit<1>  dc;
        bit<1>  p;
        bit<2>  si;
        bit<2>  r;
        bit<2>  snpadding;
        bit<16> sn;
        bit<32> teid;  
        //bit<3> rbnum;
}

//header rlc_status_h {
//        bit<1>  dc;
//        bit<3>  cpd;
//        bit<4>  snpadding;  
//        bit<16> sn;   
//        bit<1>  e;     
//        bit<7>  r;
//        bit<16> nacksn;  
//        bit<8>  nacke;
//        bit<8>  nackr;
//        bit<32> teid;  
        //bit<3> rbnum;
//}


header sdap_dl_h{
        bit<1> rdi;
        bit<1> rqi;
        bit<6> qfi;
}

//header sdap_ul_h{
//        bit<1> dc;
//        bit<1> r;
 //       bit<6> qfi;
//}

header pdcp_h{
        bit<4> dc;
        bit<4> r;
        bit<16> sn;
}

struct qos_metadata_t {

        bit<32> teid_qfi;
        bit<3> rbnumber;        
        bit<1> rqibool;        

}



const bit<16>  ETHPROTO_IPV4    = 0x0800;
const bit<8>   IPPROTO_TCP      = 6;
const bit<8>   IPPROTO_UDP      = 17;
const bit<16>   PHYS_BUFF_PORT   = 163;
const bit<16>   DOWNLINK_PORT    = 162;
const bit<16>   UPLINK_PORT      = 162;
const bit<16>  UDP_PORT_GTPU    = 2152;
const bit<16>  UDP_SPORT_RLC    = 8042;
const bit<16>  UDP_SPORT_RLCS    = 8052;
const bit<16>  UDP_SPORT_HRLC    = 8043;
const bit<16>  UDP_SPORT_HRLCS    = 8053;
const bit<16>  UDP_DPORT_RLC    = 65359;
const bit<16>  UDP_PORT_BUFFER  = 12345;
const bit<16>   UDP_PORT_HOST_GTP = 2153;
const bit<16>   UDP_PORT_HOST_all = 1234;

/* Ingress Pipe */

/* Headers to parse */
struct ingress_headers_t {
        eth_h                   eth;
        ipv4_h                  ipv4;
        udp_h                   udp;
        inneripv4_h             inneripv4;
        innerudp_h              innerudp;
        gtpu_h                  gtp;
        physical_buffer_h       buffering;
        //rlc_status_h            rlc_status;
        rlc_ack_mode_h          rlc_ack_mode;
        sdap_dl_h               sdap_dl;
        //sdap_ul_h               sdap_ul;
        pdcp_h                  pdcp;



}

/* Intermediate data available for Ingress */
struct ingress_metadata_t {
}

/* Parsing logic */
parser IngressParser(packet_in  pkt,
        out ingress_headers_t   hdr,
        inout qos_metadata_t      qos_metadata,
        inout standard_metadata_t standard_metadata)

{
        state start {
                transition parse_ethernet;
        }

        state parse_ethernet {
                pkt.extract(hdr.eth);
                transition select (hdr.eth.type){
                        ETHPROTO_IPV4   :       parse_ipv4;
                        default         :       parse_ipv4;
                }
        }

        state parse_ipv4 {
                pkt.extract(hdr.ipv4);
                transition select (hdr.ipv4.protocol){
                        IPPROTO_UDP     :       parse_udp;
                        default :       parse_udp;
                }
        }

        state parse_udp {
                pkt.extract(hdr.udp);
                transition select (hdr.udp.srcPort) {
                        UDP_PORT_GTPU   :       parse_gtp;
                        UDP_SPORT_RLC   :       parse_rlc_ack_mode;
                        UDP_PORT_HOST_GTP :       parse_gtp;
                        UDP_SPORT_HRLC :       parse_rlc_ack_mode;
                         //2153 :       p;
                        default         :       accept;
                }
        }

        state parse_gtp {
                pkt.extract(hdr.gtp);
                pkt.extract(hdr.inneripv4);
                pkt.extract(hdr.innerudp);
                        transition parse_phys_buffer;
                }
        state p {
                pkt.extract(hdr.buffering);
                }
        //// if pkt is RLC ////
        //state parse_rlc {
        //        transition select(pkt.lookahead<bit<1>>()){
        //                0       :       parse_rlc_status;
        //                1       :       parse_rlc_ack_mode;
        //        }
        //}

        //// if pkt is a RLC ordinary ////
        state parse_rlc_ack_mode {
                pkt.extract(hdr.rlc_ack_mode);
                pkt.extract(hdr.pdcp);
                pkt.extract(hdr.sdap_dl);
                pkt.extract(hdr.inneripv4);
                pkt.extract(hdr.innerudp);
                transition parse_phys_buffer;
        }


        //// if pkt is a RLC status /////
        //state parse_rlc_status {
        //        pkt.extract(hdr.rlc_status);
        //        transition parse_phys_buffer;

        //}

        //// if pkts go to buffer ////
        state parse_phys_buffer {
                pkt.extract(hdr.buffering);
                    transition accept;

        }

        //// if gtp pkts comes from buffer ////
        //state parse_phys_buffer_gtp {
        //        pkt.extract(hdr.gtp);
        //        pkt.extract(hdr.inneripv4);
        //        pkt.extract(hdr.innerudp);
        //        pkt.extract(hdr.buffering);
        //            transition accept;

        //}

        //// if rlc pkts comes from buffer ////
        //state parse_phys_buffer_rlc {
        //        pkt.extract(hdr.rlc_ack_mode);
        //        pkt.extract(hdr.pdcp);
        //        pkt.extract(hdr.sdap_dl);
        //        pkt.extract(hdr.inneripv4);
        //        pkt.extract(hdr.innerudp);
        //        pkt.extract(hdr.buffering);
        //            transition accept;

        //}


}

control Verifyc(
inout ingress_headers_t hdr,
inout qos_metadata_t            qos_metadata)
{
        apply {
                //pkt.emit(hdr);
        }
}



control Ingress1(

        /* user defined */
        inout ingress_headers_t         hdr,
        inout qos_metadata_t            qos_metadata,
        inout standard_metadata_t standard_metadata)        

        {

//////////////////////////////////////////////////////////////////////////////////////////
        action dl_put_sdap_0(rqibool rqi){
                hdr.sdap_dl.setValid();
                hdr.sdap_dl.rqi=rqi;
        }
        action dl_put_sdap_1(rqibool rqi){
                hdr.sdap_dl.setValid();
                hdr.sdap_dl.rqi=rqi;
        }

        action dl_put_pdcp_0(){
                hdr.pdcp.setValid();
        }
        action dl_put_pdcp_1(){
                hdr.pdcp.setValid();
        }

        action dl_put_rlc_am_0(){
                
                //hdr.inneripv4.setValid();
                //hdr.innerudp.setValid();
                //hdr.inneripv4 = hdr.ipv4;
                //hdr.innerudp = hdr.udp;

                hdr.rlc_ack_mode.setValid();
                hdr.rlc_ack_mode.dc=1;
                hdr.rlc_ack_mode.p=0x000;
                hdr.rlc_ack_mode.si=0x00;
                hdr.rlc_ack_mode.r=0x00;
                hdr.rlc_ack_mode.sn = 0x00; 
        }
        action dl_put_rlc_am_1(){


                hdr.rlc_ack_mode.setValid();
                hdr.rlc_ack_mode.dc=1;
                hdr.rlc_ack_mode.p=0x000;
                hdr.rlc_ack_mode.si=0x00;
                hdr.rlc_ack_mode.r=0x00;
                hdr.rlc_ack_mode.sn = 0x00; 
        }

        action dl_cloning_0(){

        const bit<32> clone_spec = 1 | (1 << 31);
        clone3(CloneType.I2E, clone_spec,standard_metadata);
            
        }
       action dl_cloning_1(){

        const bit<32> clone_spec = 1 | (1 << 31);
        clone3(CloneType.E2E, clone_spec,standard_metadata);
            
        }

        action dl_buffering_0(){
                hashp h;
                hash(h,HashAlgorithm.crc16,(bit<16>)0,
                { hdr.ipv4.srcAddr,
                  hdr.ipv4.dstAddr },(bit<16>)0x40);                
                hdr.buffering.endpoint_id = hdr.gtp.teid;
                hdr.buffering.setValid();
                hdr.buffering.ack_sn = hdr.gtp.seq_num; 
                hdr.buffering.nack_count = 0xff; //0xff MEANS NOT A STATUS MESSAGE
                hdr.buffering.pf_number=standard_metadata.ingress_port;
                hdr.udp.dstPort = UDP_PORT_BUFFER;
                standard_metadata.egress_spec= BASE_VF_PORT + (bit<16>)h;
                //standard_metadata.egress_spec= standard_metadata.ingress_port;
        }
        action dl_buffering_1(){
                hashp h;
                hash(h,HashAlgorithm.crc16,(bit<16>)0x2,
                { hdr.ipv4.srcAddr,
                  hdr.ipv4.dstAddr },(bit<16>)0x40);                
                hdr.buffering.endpoint_id = hdr.gtp.teid;
                hdr.buffering.setValid();
                hdr.buffering.ack_sn = hdr.gtp.seq_num; 
                hdr.buffering.nack_count = 0xff; //0xff MEANS NOT A STATUS MESSAGE
                hdr.buffering.pf_number=standard_metadata.ingress_port;
                hdr.udp.dstPort = UDP_PORT_BUFFER;
                standard_metadata.egress_spec= BASE_VF_PORT + (bit<16>)h;
                //standard_metadata.egress_spec= standard_metadata.ingress_port;
        }



        action assigning_drb_rqi(rbnumber rb,rqibool rqi)
        {
               qos_metadata.rbnumber=rb;
               qos_metadata.rqibool=rqi;         
        }


        table TEIDs_QFIs_to_RBs_RQI
        {
            key = { 
                hdr.gtp.teid: exact;
            }

            actions = {
                assigning_drb_rqi;
                
            }
            size =64000;
        }
////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////////////////
        action uplink(egressSpec_t port) {

        }


        action ul_remove_sdap_0(){
            hdr.sdap_dl.setInvalid();
        }
        action ul_remove_sdap_1(){
            hdr.sdap_dl.setInvalid();
        }

        action ul_remove_pdcp_0(){
            hdr.pdcp.setInvalid();
        }
        action ul_remove_pdcp_1(){
            hdr.pdcp.setInvalid();
        }

        action ul_remove_rlc_am_0(){
            hdr.rlc_ack_mode.setInvalid();
            hdr.buffering.setInvalid();
        }

        action ul_remove_rlc_am_1(){
            hdr.rlc_ack_mode.setInvalid();
            hdr.buffering.setInvalid();
        }

        action assigning_teid(teidf teid,rbnumber rb){
                //hdr.gtp.setValid();
                //hdr.gtp.teid=teid;
                hdr.gtp.flags = 0x32;
                hdr.gtp.type = 0xff;
                hdr.gtp.length = 88;
                hdr.gtp.seq_num = hdr.rlc_ack_mode.sn;
                hdr.udp.dstPort = UDP_PORT_GTPU;
                standard_metadata.egress_spec= standard_metadata.ingress_port;

        }

        table UL_ASSIGN_TEID
        {
            key = { 
                hdr.ipv4.dstAddr: exact;
            }

            actions = {
                assigning_teid;
                
            }
            size =64000;
        }

        action ul_rlc_ack_handeling_0()
        {
                  ul_remove_sdap_0();
                  ul_remove_pdcp_0();
                  ul_remove_rlc_am_0();

        }

        action ul_rlc_ack_handeling_1()
        {
                  ul_remove_sdap_1();
                  ul_remove_pdcp_1();
                  ul_remove_rlc_am_1();

        }

        apply {
                if (standard_metadata.ingress_port > 8){

                        
                        hdr.eth.smac= (bit<48>) 0x112233445566; //kau
                        hdr.eth.dmac=(bit<48>)0x3cfdfecb4349;   //kau trex
                        //standard_metadata.egress_spec= (bit<16>) hdr.buffering.pf_number;   //this is correct if you want to recieve return of cloned from vf and original on same pf (total 8MPPS)
                        standard_metadata.egress_spec= 1; //this is correct if you want to recieve return of cloned from vf and original on diff pf (each 8MPPS, total 16MPPS)

                }else if(hdr.udp.srcPort==UDP_PORT_GTPU){
                                //qos_metadata.teid_qfi= (bit<32>)  hdr.gtp.teid % 10;
                              //if (hdr.gtp.qfi<5){
                              
                              //TEIDs_QFIs_to_RBs_RQI.apply();

                              //if (qos_metadata.rbnumber==0){
                                //  hdr.gtp.setInvalid();
                                //  dl_put_sdap_0(qos_metadata.rqibool);
                                //  dl_put_pdcp_0();
                                //  dl_put_rlc_am_0();
                                dl_cloning_0();
                                     
                                  //dl_buffering_0();

                                //    macp temp;
                                //    temp= hdr.eth.smac;
                                //    hdr.eth.smac=hdr.eth.dmac;
                                //    hdr.eth.dmac=temp;

                                //   standard_metadata.egress_spec=(bit<16>) standard_metadata.ingress_port;

                            //}else if(qos_metadata.rbnumber==1){     
                            //}else {
                        if (standard_metadata.instance_type != NETRO_INSTANCE_TYPE_I2E_CLONE) {

                                // hdr.udp.srcPort= (bit<16>) UDP_PORT_HOST_GTP;
                                // hdr.buffering.setValid();
                                // hdr.buffering.pf_number= (bit<16>) standard_metadata.ingress_port;
                                // standard_metadata.egress_spec= BASE_VF_PORT + (bit<16>)(hdr.ipv4.dstAddr%8);
                                // //standard_metadata.egress_spec = BASE_VF_PORT + (bit<16>)((hdr.ipv4.dstAddr % 2) * 4);
                                // //standard_metadata.egress_spec = BASE_VF_PORT;

                        //     //} 
                        // } 
                        // else {
                                // dl_cloning_0();
                                   macp temp;
                                   temp= hdr.eth.smac;
                                   hdr.eth.smac=hdr.eth.dmac;
                                   hdr.eth.dmac=temp;
                                standard_metadata.egress_spec=(bit<16>) standard_metadata.ingress_port;

                        }        
                        

                }else if(hdr.udp.srcPort==UDP_SPORT_RLC){
               TEIDs_QFIs_to_RBs_RQI.apply();
                                //hdr.udp.srcPort= (bit<16>) UDP_SPORT_HRLCS ;
                                //hdr.buffering.setValid();
                                //hdr.buffering.pf_number= (bit<16>) standard_metadata.ingress_port;
                                //standard_metadata.egress_spec= BASE_VF_PORT + (bit<16>)(hdr.ipv4.dstAddr%8);



                }else if(hdr.udp.srcPort==UDP_SPORT_RLC){

                             //   if (hdr.rlc_ack_mode.rbnum==0){

    
                                      ul_rlc_ack_handeling_0();
                            //          standard_metadata.egress_spec= standard_metadata.ingress_port;
                                      UL_ASSIGN_TEID.apply();

                            //    }else if(hdr.rlc_ack_mode.rbnum==1){     
                   
 
                            //          ul_rlc_ack_handeling_1();
                                //      standard_metadata.egress_spec= standard_metadata.ingress_port;
                            //          UL_ASSIGN_TEID.apply();
                            //    } 


                                hdr.udp.srcPort= (bit<16>) UDP_SPORT_HRLC ;
                                hdr.buffering.setValid();
                                hdr.buffering.pf_number= (bit<16>) standard_metadata.ingress_port;
                                standard_metadata.egress_spec= BASE_VF_PORT + (bit<16>)(hdr.ipv4.dstAddr%8);


                          
                                //PFs_to_VFs_buffering.apply();
                               

                       //}else if(hdr.udp.srcPort==66666){     //IF PACKET IS RLC STATUS
                        //    PFs_to_PFs_downlink.apply();
                                    //PFs_to_VFs_buffering.apply();
                           //     PFs_to_PFs_downlink.apply(); 
                                //PFs_to_VFs_status_buffering.apply();

                        //}//else if(hdr.rlc_ack_mode.isValid()){   //IF PACKET IS RLC BUT NOT STATUS

                                //VFs_to_PFs_retransmit.apply();
                               //PFs_to_PFs_uplink.apply();
                        //}
                }

              
        }
}


/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control MyEgress(inout ingress_headers_t   hdr,
                 inout qos_metadata_t            qos_metadata,
                 inout standard_metadata_t standard_metadata) {

        apply {

                // dl_cloning_1();
                        if (standard_metadata.instance_type == NETRO_INSTANCE_TYPE_I2E_CLONE) {

                                hdr.udp.srcPort= (bit<16>) UDP_PORT_HOST_GTP;
                                hdr.buffering.setValid();
                                hdr.buffering.pf_number= (bit<16>) standard_metadata.ingress_port;
                                standard_metadata.egress_port= BASE_VF_PORT + (bit<16>)(hdr.ipv4.dstAddr%8);
                                //standard_metadata.egress_spec = BASE_VF_PORT + (bit<16>)((hdr.ipv4.dstAddr % 2) * 4);
                                //standard_metadata.egress_spec = BASE_VF_PORT;

                            //} 
                        } 
        // if (standard_metadata.instance_type == NETRO_INSTANCE_TYPE_I2E_CLONE) {

                                //    macp temp;
                                //    temp= hdr.eth.smac;
                                //    hdr.eth.smac=hdr.eth.dmac;
                                //    hdr.eth.dmac=temp;
                                
//                                 standard_metadata.egress_port=(bit<16>) standard_metadata.ingress_port;

//                         }   
     }
}

/*************************************************************************
*************   C H E C K S U M    C O M P U T A T I O N   **************
*************************************************************************/

control MyCompute(inout ingress_headers_t   hdr,
                  inout qos_metadata_t            qos_metadata) 
{
     apply {

    }
}

/*********************s****************************************************
***********************  D E P A R S E R  *******************************
*************************************************************************/

control MyDeparser(packet_out pkt, 
                   in ingress_headers_t   hdr
                   )
 {
    apply {
            pkt.emit(hdr.eth);
            pkt.emit(hdr.ipv4);
            pkt.emit(hdr.udp);
            
            pkt.emit(hdr.gtp);
            pkt.emit(hdr.inneripv4);
            pkt.emit(hdr.innerudp);
            pkt.emit(hdr.buffering);

            pkt.emit(hdr.rlc_ack_mode);
            //pkt.emit(hdr.rlc_status);
            pkt.emit(hdr.pdcp);
            pkt.emit(hdr.sdap_dl);
    }
}


V1Switch(IngressParser(), Verifyc(),Ingress1(),  MyEgress(),MyCompute(), MyDeparser()) main;
