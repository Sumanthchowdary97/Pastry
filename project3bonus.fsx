#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"


open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics                                                      // importing Akka.Configuration through NuGet Manager
open Akka.TestKit

let configuration =                                                                                 // Setting configuration to suppress dead letters
    ConfigurationFactory.ParseString(                                                               // Num dead letters to zero indicates no dead letter warnings are displayed after exit
        @"akka {
            log-dead-letters-during-shutdown : off
            log-dead-letters = 0                                                                    
        }")

let system = ActorSystem.Create("system",configuration)

let args = Environment.GetCommandLineArgs()                                                         // Fetching Environment variables

let mutable NumNodes = 1000
let mutable NumRequests = 60
let mutable NumFailures = 10

if args.Length > 3 then
    NumNodes <- int args.[3]
    NumRequests <- int args.[4]
    NumFailures <- int args.[5]
let rand = System.Random()
let timer = new System.Diagnostics.Stopwatch()
let mutable GlobalStop = false

printfn ""

type Message=
    |Init 
    |PrimaryJoin of int[]
    |JoinComplete
    |InitiateRouting
    |SecondaryJoin
    |NodeNotInBothLeafSet
    |RouteNotInBoth
    |RoutingComplete of int * int * int
    |Route of string * int * int * int
    |UpdateRow of int * int[]
    |UpdateLeaf of int[]
    |UpdateNodeInfo of int
    |Report 
    |SetupFailOver
    |KillNode 
    |RemoveNode of int
    |RequestLeafWithout of int
    |LeafRecover of int[] * int
    |TableLookUp of int * int
    |UpdateTable of int * int * int
    
let Child (ActorID:int,b:int) (mailbox :Actor<_>) =
    let mutable numOfBack = 0
    let mutable NodeIDSpace = Math.Pow(4 |> float,b |> float) |> int 

    let mutable SmallerNodeLeafSet = [||]
    let mutable LargerNodeLeafSet = [||]

    let ConvertToBase4String(raw: int, length: int): string =
        let ConvertBigIntToDigits b source =
            let rec loop (b : int) num digits =
                let (quotient, remainder) = bigint.DivRem(num, bigint b)
                match quotient with
                | zero when zero = 0I -> int remainder :: digits
                | _ -> loop b quotient (int remainder :: digits)
            loop b source []
        let Stringify source =
            source
            |> List.map (fun (x : int) -> x.ToString("X").ToLowerInvariant())
            |> String.concat ""
        let printBigintAsHex source :string=
            let bigintToHex = ConvertBigIntToDigits 4
            source |> bigintToHex |>  Stringify
        let mutable str = printBigintAsHex (bigint raw)
        let mutable Difference = length - str.Length
        if (Difference > 0) then
            let mutable idx = 0
            while (idx < Difference) do
                str <- "0" + str
                idx <- idx+1
        str
    
    let CommonPrefix(str1: string, str2: string): int = 
        let mutable idx = 0
        while idx < str1.Length && str1.[idx] = str2.[idx] do
            idx <- idx+1

        idx
    let mutable RoutingTable = [|for i in 0..b-1 -> [||] |]
    for i in 0..b-1 do
       RoutingTable.[i] <- [|-1;-1;-1;-1|]
    let UpdateBuffer (Buff: int[]) = 
        for i in Buff do 
            if i > ActorID && not (Array.contains i LargerNodeLeafSet) then //i may be added to larger leaf
                if LargerNodeLeafSet.Length < 4 then
                    LargerNodeLeafSet <- Array.append LargerNodeLeafSet [|i|]
                else 
                    if i < Array.max LargerNodeLeafSet then
                        LargerNodeLeafSet <- Array.filter (fun elem -> elem <>  Array.max LargerNodeLeafSet) LargerNodeLeafSet
                        LargerNodeLeafSet <- Array.append LargerNodeLeafSet [|i|]
              
            elif i < ActorID && not (Array.contains i SmallerNodeLeafSet) then//i may be added to less leaf
                if SmallerNodeLeafSet.Length < 4 then
                    SmallerNodeLeafSet <- Array.append SmallerNodeLeafSet [|i|]
                else
                    if i > Array.min SmallerNodeLeafSet then
                        SmallerNodeLeafSet <- Array.filter (fun elem -> elem <>  Array.min SmallerNodeLeafSet) SmallerNodeLeafSet
                        SmallerNodeLeafSet <- Array.append SmallerNodeLeafSet [|i|]
            
          
            let mutable Prefix = CommonPrefix((ConvertToBase4String(ActorID,b)),(ConvertToBase4String(i,b)))
          
            if RoutingTable.[Prefix].[int (string (ConvertToBase4String(i,b)).[Prefix])] = -1 then
                RoutingTable.[Prefix].[int (string (ConvertToBase4String(i,b)).[Prefix])] <- i
        
    
    let UpdateNode(NodeID: int) = 
        if (NodeID > ActorID && not (Array.contains NodeID LargerNodeLeafSet)) then //i may be added to larger leaf
            if (LargerNodeLeafSet.Length < 4) then
                LargerNodeLeafSet <- Array.append LargerNodeLeafSet [|NodeID|] 
            else 
                if (NodeID < Array.max LargerNodeLeafSet) then
                    LargerNodeLeafSet <- Array.filter (fun elem -> elem <>  Array.max LargerNodeLeafSet) LargerNodeLeafSet
                    LargerNodeLeafSet <- Array.append LargerNodeLeafSet [|NodeID|]
        elif (NodeID < ActorID && not (Array.contains NodeID SmallerNodeLeafSet)) then //i may be added to less leaf
            if (SmallerNodeLeafSet.Length < 4) then
                SmallerNodeLeafSet <- Array.append SmallerNodeLeafSet [|NodeID|] 
            else 
                if (NodeID > Array.min SmallerNodeLeafSet) then
                    SmallerNodeLeafSet <- Array.filter (fun elem -> elem <>  Array.min SmallerNodeLeafSet) SmallerNodeLeafSet 
                    SmallerNodeLeafSet <- Array.append SmallerNodeLeafSet [|NodeID|] 
            
        
        let Prefix = CommonPrefix((ConvertToBase4String(ActorID,b)),(ConvertToBase4String(NodeID,b)))
        if RoutingTable.[Prefix].[int (string (ConvertToBase4String(NodeID,b)).[Prefix])] = -1 then
           RoutingTable.[Prefix].[int (string (ConvertToBase4String(NodeID,b)).[Prefix])] <- NodeID


    let rec loop () = actor {
        let! msg = mailbox.Receive ()

        match msg with
        | Report ->
            numOfBack <- numOfBack-1
            if (numOfBack = 0) then
                mailbox.Context.Parent <! JoinComplete
        | UpdateNodeInfo(newNodeID) ->
            UpdateNode newNodeID
            mailbox.Sender () <! Report 
         
        | KillNode ->
            select ("/user/master/*") mailbox.Context.System  <! RemoveNode(ActorID)
            mailbox.Context.System.Stop mailbox.Self
        
        | RequestLeafWithout(NodeID) ->
            let mutable temp = [||]
            temp <- Array.append temp SmallerNodeLeafSet
            temp <- Array.append temp LargerNodeLeafSet 
            temp <- Array.filter (fun elem -> elem <>  NodeID) temp
            mailbox.Sender () <! LeafRecover(Array.copy temp, NodeID)        
        | PrimaryJoin(firstGroup) ->
            let q = Array.filter (fun elem -> elem <> ActorID) firstGroup 
            UpdateBuffer q
            for i = 0 to b-1 do
                RoutingTable.[i].[ int (string (ConvertToBase4String(ActorID,b)).[i])] <- ActorID
            mailbox.Sender () <! JoinComplete
            
        | UpdateLeaf(Leaf) ->
            UpdateBuffer Leaf
            for i in SmallerNodeLeafSet do
                numOfBack <- numOfBack+1
                select ("/user/master/"+ string i) mailbox.Context.System <! UpdateNodeInfo(ActorID)
            for i in LargerNodeLeafSet do
                numOfBack <- numOfBack+1
                select ("/user/master/"+ string i) mailbox.Context.System <! UpdateNodeInfo(ActorID)
            for i = 0 to b-1 do
                let mutable j = 0
                for j = 0 to 3 do
                    if (RoutingTable.[i].[j] <> -1) then
                        numOfBack <- numOfBack+1
                        select ("/user/master/"+ string RoutingTable.[i].[j]) mailbox.Context.System <! UpdateNodeInfo(ActorID)
            for i = 0 to b-1 do
                RoutingTable.[i].[ int (string (ConvertToBase4String(ActorID,b)).[i])] <- ActorID
        | TableLookUp(Prefix, Col) ->
            if (RoutingTable.[Prefix].[Col] <> -1) then
                mailbox.Sender () <! UpdateTable(Prefix, Col, RoutingTable.[Prefix].[Col])      
        | InitiateRouting ->
            for i = 1 to NumRequests do
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds 1000., mailbox.Self, Route("Route", ActorID, rand.Next(0,NodeIDSpace), -1))
        | UpdateTable(Row, Col, newID) ->
            if (RoutingTable.[Row].[Col] = -1) then
                RoutingTable.[Row].[Col] <- newID
        | UpdateRow(RowNo, NewRow) ->
            for i = 0 to 3 do
                if (RoutingTable.[RowNo].[i] = -1) then
                    RoutingTable.[RowNo].[i] <- NewRow.[i]
                    
        |Route(msg, RequestFromID, RequestToID, Hops) ->
            if (msg = "Join") then
                let Prefix = CommonPrefix((ConvertToBase4String(ActorID,b)),(ConvertToBase4String(RequestToID,b)))
                if (Hops = -1 && Prefix > 0) then
                    for i = 0 to Prefix-1 do  
                        select ("/user/master/"+ string RequestToID) mailbox.Context.System <! UpdateRow(i, Array.copy RoutingTable.[i])
                select ("/user/master/"+ string RequestToID) mailbox.Context.System <! UpdateRow(Prefix, Array.copy RoutingTable.[Prefix])
                if ((SmallerNodeLeafSet.Length > 0 && RequestToID >= Array.min SmallerNodeLeafSet && RequestToID <= ActorID) || (LargerNodeLeafSet.Length > 0 && RequestToID <= Array.max LargerNodeLeafSet && RequestToID >= ActorID)) then //In larger leaf set
                    let mutable diff = NodeIDSpace + 10
                    let mutable nearest = -1
                    if (RequestToID < ActorID) then 
                        for i in SmallerNodeLeafSet do
                            if (Math.Abs(RequestToID - i) < diff) then
                                nearest <- i
                                diff <- Math.Abs(RequestToID - i)
                    else  
                        for i in LargerNodeLeafSet do
                            if (Math.Abs(RequestToID - i) < diff) then
                                nearest <- i
                                diff <- Math.Abs(RequestToID - i)
                    if (Math.Abs(RequestToID - ActorID) > diff) then 
                        select ("/user/master/"+ string nearest) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                    else  
                        let mutable allLeaf = [||]
                        allLeaf <- Array.append allLeaf [|ActorID|]
                        allLeaf <- Array.append allLeaf SmallerNodeLeafSet 
                        allLeaf <- Array.append allLeaf LargerNodeLeafSet
                        select ("/user/master/"+ string RequestToID) mailbox.Context.System <! UpdateLeaf(allLeaf) //Give leaf set info
                elif (SmallerNodeLeafSet.Length < 4 && SmallerNodeLeafSet.Length > 0 && RequestToID < (Array.min SmallerNodeLeafSet)) then
                  select ("/user/master/"+ string (Array.min SmallerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                elif (LargerNodeLeafSet.Length < 4 && LargerNodeLeafSet.Length > 0 && RequestToID > (Array.max LargerNodeLeafSet)) then
                  select ("/user/master/"+ string (Array.max LargerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                elif ((SmallerNodeLeafSet.Length = 0 && RequestToID < ActorID) || (LargerNodeLeafSet.Length = 0 && RequestToID > ActorID)) then
                    let mutable Leaf = [||]
                    Leaf <- Array.append Leaf [|ActorID|]
                    Leaf <- Array.append Leaf SmallerNodeLeafSet 
                    Leaf <- Array.append Leaf LargerNodeLeafSet 
                    select ("/user/master/"+ string RequestToID) mailbox.Context.System <! UpdateLeaf(Leaf)  //Give leaf set info  
                elif (RoutingTable.[Prefix].[ int (string (ConvertToBase4String(RequestToID,b)).[Prefix])] <> -1) then //Not in leaf set, try routing table
                    select ("/user/master/"+ string RoutingTable.[Prefix].[ int (string (ConvertToBase4String(RequestToID,b)).[Prefix])]) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                else 
                    let mutable diff = NodeIDSpace + 10
                    let mutable nearest = -1
                    for i = 0 to 3 do
                        if ((RoutingTable.[Prefix].[i] <> -1) && (Math.Abs(RoutingTable.[Prefix].[i] - RequestToID) < diff)) then
                            diff <- Math.Abs(RoutingTable.[Prefix].[i] - RequestToID)
                            nearest <- RoutingTable.[Prefix].[i]   
                    if (nearest <> -1) then
                        if (nearest = ActorID) then
                            if (RequestToID > ActorID) then //Not in both
                                select ("/user/master/"+ string (Array.max LargerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                                mailbox.Context.Parent <! NodeNotInBothLeafSet
                            elif (RequestToID < ActorID) then
                                select ("/user/master/"+ string (Array.min SmallerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                                mailbox.Context.Parent <! NodeNotInBothLeafSet
                            else 
                                printfn "NO!"
                        else 
                            select ("/user/master/"+ string nearest) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
            elif (msg = "Route") then 
                if (ActorID = RequestToID) then
                    mailbox.Context.Parent <! RoutingComplete(RequestFromID, RequestToID, Hops + 1)
                else 
                    let samePre = CommonPrefix((ConvertToBase4String(ActorID,b)),(ConvertToBase4String(RequestToID,b)))
                    if ((SmallerNodeLeafSet.Length > 0 && RequestToID >= Array.min SmallerNodeLeafSet && RequestToID < ActorID) || (LargerNodeLeafSet.Length > 0 && RequestToID <= Array.max LargerNodeLeafSet && RequestToID > ActorID)) then //In larger leaf set
                        let mutable diff = NodeIDSpace + 10
                        let mutable nearest = -1
                        if (RequestToID < ActorID) then 
                            for i in SmallerNodeLeafSet do
                                if (Math.Abs(RequestToID - i) < diff) then
                                    nearest <- i
                                    diff <- Math.Abs(RequestToID - i)
                        else  
                            for i in LargerNodeLeafSet do
                                if (Math.Abs(RequestToID - i) < diff) then
                                    nearest <- i
                                    diff <- Math.Abs(RequestToID - i)
                        if (Math.Abs(RequestToID - ActorID) > diff) then 
                            select ("/user/master/"+ string nearest) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                        else  
                            mailbox.Context.Parent <! RoutingComplete(RequestFromID, RequestToID, Hops + 1)
                    elif (SmallerNodeLeafSet.Length < 4 && SmallerNodeLeafSet.Length > 0 && RequestToID < Array.min SmallerNodeLeafSet) then
                        select ("/user/master/"+ string (Array.min SmallerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                    elif (LargerNodeLeafSet.Length < 4 && LargerNodeLeafSet.Length > 0 && RequestToID > Array.max LargerNodeLeafSet) then
                        select ("/user/master/"+ string (Array.max LargerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                    elif ((SmallerNodeLeafSet.Length = 0 && RequestToID < ActorID) || (LargerNodeLeafSet.Length = 0 && RequestToID > ActorID)) then
                        mailbox.Context.Parent <! RoutingComplete(RequestFromID, RequestToID, Hops + 1)
                    elif (RoutingTable.[samePre].[ int (string (ConvertToBase4String(RequestToID,b)).[samePre])] <> -1) then //Not in leaf set, try routing table
                        select ("/user/master/"+ string RoutingTable.[samePre].[ int (string (ConvertToBase4String(RequestToID,b)).[samePre])]) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                    else 
                        let mutable diff = NodeIDSpace + 10
                        let mutable nearest = -1
                        for i = 0 to 3 do
                            if ((RoutingTable.[samePre].[i] <> -1) && (Math.Abs(RoutingTable.[samePre].[i] - RequestToID) < diff)) then
                                diff <- Math.Abs(RoutingTable.[samePre].[i] - RequestToID)
                                nearest <- RoutingTable.[samePre].[i] 
                        if (nearest <> -1) then
                            if (nearest = ActorID) then
                                if (RequestToID > ActorID) then //Not in both
                                    select ("/user/master/"+ string (Array.max LargerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                                    mailbox.Context.Parent <! RouteNotInBoth
                                elif (RequestToID < ActorID) then
                                    select ("/user/master/"+ string (Array.min SmallerNodeLeafSet)) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
                                    mailbox.Context.Parent <! RouteNotInBoth
                                else 
                                    printfn "NO!"
                            else 
                                select ("/user/master/"+ string nearest) mailbox.Context.System <! Route(msg, RequestFromID, RequestToID, Hops + 1)
        | RemoveNode (NodeID) ->
            if (NodeID > ActorID && (Array.contains NodeID LargerNodeLeafSet)) then //In larger leaf
                LargerNodeLeafSet <- Array.filter (fun elem -> elem <>  NodeID) LargerNodeLeafSet
                if (LargerNodeLeafSet.Length > 0) then
                    select ("/user/master/"+ string (Array.max LargerNodeLeafSet)) mailbox.Context.System <! RequestLeafWithout(NodeID)
            if (NodeID < ActorID && (Array.contains NodeID SmallerNodeLeafSet)) then //In less leaf
                SmallerNodeLeafSet <- Array.filter (fun elem -> elem <>  NodeID) SmallerNodeLeafSet
                if (SmallerNodeLeafSet.Length > 0) then
                    select ("/user/master/"+ string (Array.min SmallerNodeLeafSet)) mailbox.Context.System <! RequestLeafWithout(NodeID)
            let Prefix = CommonPrefix(ConvertToBase4String(ActorID, b), ConvertToBase4String(NodeID, b))
            if (RoutingTable.[Prefix].[int (string (ConvertToBase4String(NodeID, b)).[Prefix])] = NodeID) then
                RoutingTable.[Prefix].[int (string (ConvertToBase4String(NodeID, b)).[Prefix])] <- -1
                for i = 0 to 3 do
                    if (RoutingTable.[Prefix].[i] <> ActorID && RoutingTable.[Prefix].[i] <> NodeID && RoutingTable.[Prefix].[i] <> -1) then
                        select ("/user/master/"+ string RoutingTable.[Prefix].[i]) mailbox.Context.System <!
                        TableLookUp(Prefix, int (string (ConvertToBase4String(NodeID, b)).[Prefix]))
                  

        | LeafRecover(newlist, theDead) ->
            for i in newlist do
                if (i > ActorID && not(Array.contains i LargerNodeLeafSet)) then 
                    if (LargerNodeLeafSet.Length < 4) then
                        LargerNodeLeafSet <- Array.append LargerNodeLeafSet [|i|]
                    else 
                        if (i < (Array.max LargerNodeLeafSet)) then
                            LargerNodeLeafSet <- Array.filter (fun elem -> elem <>  Array.max LargerNodeLeafSet) LargerNodeLeafSet
                            LargerNodeLeafSet <- Array.append LargerNodeLeafSet [|i|]
                elif (i < ActorID && not(Array.contains i SmallerNodeLeafSet)) then //i may be added to less leaf
                    if (SmallerNodeLeafSet.Length < 4) then
                        SmallerNodeLeafSet <- Array.append SmallerNodeLeafSet [|i|]
                    else 
                        if (i > (Array.min SmallerNodeLeafSet)) then
                            SmallerNodeLeafSet <- Array.filter (fun elem -> elem <>  Array.min SmallerNodeLeafSet) SmallerNodeLeafSet
                            SmallerNodeLeafSet <- Array.append SmallerNodeLeafSet [|i|]
        | _ -> 
            printfn "I am not supposed to receive this!!"
        return! loop ()
    }
    loop ()

let Master (mailbox :Actor<_>) =
    let mutable NodesNotInBoth = 0
    let mutable TotalRouted = 0
    let mutable b = Math.Ceiling(Math.Log(NumNodes |> float)/Math.Log(4 |> float) |> float) |> int

    let mutable NodeIDSpace = Math.Pow(4 |> float,b |> float) |> int 
    let mutable NodeList = [| for i in 0 .. NodeIDSpace-1 -> i |]

    let shuffle RandomList =
        let swap (RandomList: _[]) x y =
            let tmp = RandomList.[x]
            RandomList.[x] <- RandomList.[y]
            RandomList.[y] <- tmp
        Array.iteri (fun j _ -> swap RandomList j (rand.Next(j, Array.length RandomList))) RandomList

    shuffle NodeList

    let mutable SizeOfGroup = if NumNodes <= 1024 then NumNodes else 1024

    let mutable InitialGroup = [| for i in 0 .. SizeOfGroup-1 -> NodeList.[i] |]
    
    printfn "Node ID Space:0 ~ %i" (NodeIDSpace-1)
    printfn "Log value: %i" b

    let mutable NodesJoined = 0
    
    let mutable TotalHops = 0
    let mutable TotalRoutesNotInBoth = 0
    
    let rec loop () = actor {
        let! message = mailbox.Receive ()
        match message with
        | InitiateRouting ->
            printfn "Initiating Route"
            select ("/user/master/*") mailbox.Context.System  <! InitiateRouting
        | SetupFailOver ->
            for i = 0 to NumFailures-1 do
                select ("/user/master/"+ (string NodeList.[i])) mailbox.Context.System <! KillNode
            mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds 1000., mailbox.Self, InitiateRouting)
        | NodeNotInBothLeafSet ->
            NodesNotInBoth <- NodesNotInBoth+1
        | RouteNotInBoth ->
            TotalRoutesNotInBoth <- TotalRoutesNotInBoth+1
        | JoinComplete ->
            NodesJoined <- NodesJoined+1
            if NodesJoined = SizeOfGroup then
                if NodesJoined >= NumNodes then
                    mailbox.Self <! SetupFailOver
                else 
                    mailbox.Self <! SecondaryJoin
            if NodesJoined > SizeOfGroup then
                if NodesJoined = NumNodes then
                    mailbox.Self <! SetupFailOver
                else 
                    mailbox.Self <! SecondaryJoin
        | SecondaryJoin ->
            let startID = NodeList.[rand.Next(0, NodesJoined)]
            select ("/user/master/"+ string startID) mailbox.Context.System <! Route("Join", startID, NodeList.[NodesJoined], -1)
        | RoutingComplete(RequestFromID, RequestToID, Hops) ->
            TotalRouted <- TotalRouted + 1
            TotalHops <- TotalHops + Hops
            for n = 1 to 10 do
                if (TotalRouted = (NumNodes-NumFailures) * NumRequests * n / 10) then
                    printfn "Routing ... "
            if TotalRouted >= ((NumNodes-NumFailures) * NumRequests) && not GlobalStop then
                GlobalStop <- true
                printfn "Total Routes: %i" TotalRouted
                printfn "Total Hops: %i" TotalHops
                printfn "Average hops per route: %f" ((TotalHops|>double)/(TotalRouted |> double))
                mailbox.Context.System.Terminate ()
        | Init ->
            [for i in 0..NumNodes do yield (spawn mailbox.Context (string NodeList.[i]) (Child(NodeList.[i], b)))]
            printfn "Initiating Join"
            for i = 0 to SizeOfGroup-1 do
                select ("/user/master/"+ (string NodeList.[i])) mailbox.Context.System <! PrimaryJoin(Array.copy InitialGroup)
        | _ -> failwith("Unknown Message")
        return! loop ()
    }
    loop()
let MasterRef = spawn system "master" Master
if NumFailures <= NumNodes then
    MasterRef <! Init
else
    printfn "Node failures cannot exceed total number of nodes"
system.WhenTerminated.Wait ()