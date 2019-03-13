
Design of MQTT Bridge
=====================

The *MQTT Bridge* primarily works is forwarding a thrid MQTT broker message to EMQ X

1. 1-1 forwarding supported
2. Target broker cluster supported (works well on the target cluster supporting shared subscription)
3. 


Forward Logic
--------------------

         Msg -> [  A-Broker  ]



         |---------------|
         |               |-------->
         |    A-Broker   | 
         |               |-------->
         |---------------|


                           Subscribe
         Work Process 1   ----------->
         Work Process 2   ----------->   [ Topic A / Broker 1 ] 
              ...                        [ Topic B / Broker 1 ]
         Work Process N   ----------->   [ Topic C / Broker 2 ]
         Work Process M   ----------->


                   /--- Broker 1
        Pub-Msg   /

        Pub-Msg   ----- Broker 2



