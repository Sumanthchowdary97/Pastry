#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"

open System                                                                                         
open System.Security.Cryptography
open Akka.Actor                                                                                     // Akka actor library
open Akka.Actor
open Akka.Configuration                                                                             // Akka Configuration
open Akka.FSharp                                                                                    // Akka.FSharp library    
open Akka.TestKit                                                                                   // Akka Test Kit


let configuration =                                                                                 // Setting configuration to suppress dead letters
    ConfigurationFactory.ParseString(                                                               // Num dead letters to zero indicates no dead letter warnings are displayed after exit
        @"akka {
            log-dead-letters-during-shutdown : off
            log-dead-letters = 0                                                                    
        }")

let system = ActorSystem.Create("system", configuration)                                            // Creating Akka system with the configuration settings

let args = Environment.GetCommandLineArgs()                                                         // Fetching Environment variables

let rand = System.Random()                                                                             // Setting random state to 777

let mutable GlobalStop = false

type Message =
    | Route of string * int * int * int
    | Join of array<int>
    | UpdateRow of int * array<int>
    | AddLeaf of array<int>
    | UpdateNodeInfo of int
    | RouteFinish of int*int*int
    | Go
    | TestMSG
    | Start
    | GoJoin
    | BeginRouting
    | SecondJoin
    | RequestLeaf
    | Ack
    | JoinFinish
    | Report
    | NotInBoth
    | RouteNotInBoth

let mutable NumNodes = 1000
let mutable NumRequests = 1

if args.Length > 3 then
    NumNodes <- int args.[3]
    NumRequests <- int args.[4]

printfn "Number of nodes: %i" NumNodes

printfn "Number of Requests: %i" NumRequests

let Child(ActorID : int , b : int) (mailbox: Actor<_>) =
    let mutable SmallerLeafSet = [||]
    let mutable LargerLeafSet = [||]
    let mutable RoutingTable  = [| for i in 0..b-1 -> [||] |]
    let mutable numOfBack = 0
    let NodeIDSpace = Math.Pow(4.0, b|>double) |> int
    
    //let mutable i = 0
    let InitRoutingTable() =
        for i in 0..(b-1) do
            RoutingTable.[i] <- [|-1;-1;-1;-1|]
    
    InitRoutingTable()
    
    let ConvertToBase4String(raw:int, length:int):string =
        let Stringify source =
            source
            |> List.map (fun (x : int) -> x.ToString("X").ToLowerInvariant())
            |> String.concat ""
        let printBigintAsHex source:string =
            let bigintToDigits b source =
                let rec loop (b : int) num digits =
                    let (quotient, remainder) = bigint.DivRem(num, bigint b)
                    match quotient with
                    | zero when zero = 0I -> int remainder :: digits
                    | _ -> loop b quotient (int remainder :: digits)
                loop b source []
            let bigintToHex = bigintToDigits 4
            source |> bigintToHex |>  Stringify
        let mutable str=printBigintAsHex (bigint raw)
        let mutable diff=length-str.Length
        if(diff>0) then
            let mutable j=0
            while(j<diff) do
                str<-"0"+str
                j<-j+1
        str
    
    let CommonPrefix(String1: string, String2: string): int = 
        let mutable idx=0
        while(idx<String1.Length && ((String1.ToCharArray().[idx])=(String2.ToCharArray().[idx]))) do
            idx<-idx+1
        idx

    let addBuffer(all: array<int>):unit = 
        for i in all do 
            if (i > ActorID && not (Array.contains i LargerLeafSet)) then
                if (LargerLeafSet.Length < 4) then
                    LargerLeafSet <- Array.append LargerLeafSet [|i|]
                else 
                    if i < Array.max LargerLeafSet then
                        LargerLeafSet <- LargerLeafSet |> Array.filter ((<>) (Array.max LargerLeafSet))
                        LargerLeafSet <- Array.append LargerLeafSet [|i|]

            elif (i< ActorID && not (Array.contains i SmallerLeafSet)) then 
                if SmallerLeafSet.Length < 4 then
                    SmallerLeafSet <- Array.append SmallerLeafSet [|i|]
                else 
                    if i > (Array.min SmallerLeafSet) then
                        SmallerLeafSet <- SmallerLeafSet |> Array.filter ((<>) (Array.min SmallerLeafSet))
                        SmallerLeafSet <- Array.append SmallerLeafSet [|i|]

            let samePrefix = CommonPrefix(ConvertToBase4String(ActorID, b), ConvertToBase4String(i, b))
            if ((RoutingTable.[samePrefix].[int (string (ConvertToBase4String(i,b).ToCharArray().[samePrefix]))])= -1) then
                RoutingTable.[samePrefix].[int (string (ConvertToBase4String(i,b).ToCharArray().[samePrefix]))] <- i

    let addOne(node: int): unit =
        if (node > ActorID && not (Array.contains node LargerLeafSet)) then
            if LargerLeafSet.Length < 4 then
                LargerLeafSet <- Array.append LargerLeafSet [|node|]
            else 
                if node < Array.max LargerLeafSet then
                    LargerLeafSet <- LargerLeafSet |> Array.filter ((<>) (Array.max LargerLeafSet))
                    LargerLeafSet <- Array.append LargerLeafSet [|node|]

        elif node < ActorID && not (Array.contains node SmallerLeafSet) then
            if SmallerLeafSet.Length < 4 then
                    SmallerLeafSet <- Array.append SmallerLeafSet [|node|]
            else 
                if node > (Array.min SmallerLeafSet) then
                    SmallerLeafSet <- SmallerLeafSet |> Array.filter ((<>) (Array.min SmallerLeafSet))
                    SmallerLeafSet <- Array.append SmallerLeafSet [|node|]
                    
        let Prefix = CommonPrefix(ConvertToBase4String(ActorID, b), ConvertToBase4String(node, b))
        if ((RoutingTable.[Prefix].[int (string (ConvertToBase4String(node,b).ToCharArray().[Prefix]))])= -1) then
            RoutingTable.[Prefix].[int (string (ConvertToBase4String(node,b).ToCharArray().[Prefix]))] <- node
    
    
    let rec receive() = actor {
        let! msg = mailbox.Receive()
        match msg with
        | Join(firstGroup) ->
            let newGroup = firstGroup |> Array.filter((<>) ActorID)
            addBuffer(newGroup)
            for i in 0..b-1 do
                RoutingTable.[i].[int (ConvertToBase4String(ActorID,b).ToCharArray().[i].ToString())] <- ActorID
            mailbox.Sender() <! JoinFinish
        | Route(msg, RequestFromID, RequestToID, Hops) ->
            if(msg = "Join") then
                let Prefix = CommonPrefix(ConvertToBase4String(ActorID, b), ConvertToBase4String(RequestToID, b))
                if Hops = -1 && Prefix > 0 then
                    for i in 0..Prefix-1 do
                        mailbox.Context.System.ActorSelection("/user/master/"+(string RequestToID)) <! UpdateRow(i, Array.copy RoutingTable.[i])
                mailbox.Context.System.ActorSelection("user/master/" + (string RequestToID)) <! UpdateRow(Prefix, Array.copy RoutingTable.[Prefix])
                if (SmallerLeafSet.Length >0 && RequestToID >= (Array.min SmallerLeafSet) && RequestToID <= ActorID) || (LargerLeafSet.Length>0 && RequestToID <= (Array.max LargerLeafSet) && RequestToID >= ActorID) then
                    let mutable diff = NodeIDSpace + 10
                    let mutable nearest = -1
                    if RequestToID < ActorID then
                        for i in SmallerLeafSet do
                            if Math.Abs(RequestToID-i) < diff then
                                nearest = i
                                diff = Math.Abs(RequestToID - i)
                                
                    else
                        for i in LargerLeafSet do
                            if Math.Abs(RequestToID-i) < diff then
                                nearest <- i
                                diff = Math.Abs(RequestToID - i)
                    if Math.Abs(RequestToID - ActorID) > diff then
                        mailbox.Context.System.ActorSelection("/user/master/" + (string nearest)) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                    else
                        let mutable allLeaf = [||]
                        allLeaf <- Array.append (Array.append [|ActorID|] SmallerLeafSet) LargerLeafSet
                        mailbox.Context.System.ActorSelection("/user/master/"+(string RequestToID)) <! AddLeaf(allLeaf)
                elif SmallerLeafSet.Length < 4 && SmallerLeafSet.Length>0 && RequestToID < (Array.min SmallerLeafSet) then
                    mailbox.Context.System.ActorSelection("/user/master/"+ (string (Array.min SmallerLeafSet))) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                elif LargerLeafSet.Length<4 && LargerLeafSet.Length > 0 && RequestToID > (Array.max LargerLeafSet) then
                    mailbox.Context.System.ActorSelection("/user/master/"+(string (Array.max LargerLeafSet))) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                elif (SmallerLeafSet.Length = 0 && RequestToID < ActorID) || LargerLeafSet.Length = 0 && RequestToID > ActorID then
                    let mutable allLeaf = [||]
                    allLeaf <- Array.append (Array.append [|ActorID|] SmallerLeafSet) LargerLeafSet
                    mailbox.Context.System.ActorSelection("/user/master/" + (string RequestToID)) <! AddLeaf(allLeaf)
                elif (RoutingTable.[Prefix].[int (ConvertToBase4String(RequestToID,b).ToCharArray().[Prefix].ToString())] <> -1) then
                    mailbox.Context.System.ActorSelection("/user/master/"+string (RoutingTable.[Prefix].[int (ConvertToBase4String(RequestToID,b).ToCharArray().[Prefix].ToString())]))  <! Route(msg,RequestFromID,RequestToID,Hops+1)   
                elif (RequestToID > ActorID) then
                    mailbox.Context.System.ActorSelection("/user/master/" + string (Array.max (LargerLeafSet))) <! Route(msg,RequestFromID,RequestToID,Hops+1)
                    mailbox.Context.Parent <! NotInBoth
                elif (RequestFromID < ActorID) then
                    mailbox.Context.System.ActorSelection("/user/master/" + string (Array.min (SmallerLeafSet))) <! Route(msg,RequestFromID,RequestToID,Hops+1)
                    mailbox.Context.Parent <! NotInBoth   
                else 
                    printfn "Impossible"
                           
            elif msg = "Route" then
                if ActorID = RequestToID then
                    mailbox.Context.Parent <! RouteFinish(RequestFromID, RequestToID, Hops+1)
                else
                    let samePre = CommonPrefix(ConvertToBase4String(ActorID, b), ConvertToBase4String(RequestToID, b))
                    if ((SmallerLeafSet.Length > 0 && RequestToID >= (Array.min SmallerLeafSet) && RequestToID < ActorID) || (LargerLeafSet.Length >0 && RequestToID <= (Array.max LargerLeafSet) && RequestToID > ActorID)) then
                        let mutable Difference = NodeIDSpace + 10
                        let mutable nearest = -1
                        if RequestToID < ActorID then
                            for i in SmallerLeafSet do
                                if Math.Abs(RequestToID - i) < Difference then
                                    nearest <- i
                                    Difference = Math.Abs(RequestToID - i)
                        else
                            for i in LargerLeafSet do
                                if Math.Abs(RequestToID - i) < Difference then
                                    nearest <- i
                                    Difference = Math.Abs(RequestToID - i)
                        if Math.Abs(RequestToID - ActorID) > Difference then
                            mailbox.Context.System.ActorSelection("/user/master/"+ (string nearest)) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                        else
                            mailbox.Context.Parent <! RouteFinish(RequestFromID, RequestToID, Hops+1)
                    elif SmallerLeafSet.Length<4 && SmallerLeafSet.Length > 0 && RequestToID < (Array.min SmallerLeafSet) then
                        mailbox.Context.System.ActorSelection("/user/master/"+(string (Array.min SmallerLeafSet))) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                    elif LargerLeafSet.Length < 4  && LargerLeafSet.Length>0 && RequestToID > (Array.max LargerLeafSet) then
                        mailbox.Context.System.ActorSelection("/user/master"+(string (Array.max LargerLeafSet))) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                    elif (SmallerLeafSet.Length = 0 && RequestToID < ActorID) || (LargerLeafSet.Length = 0 && RequestToID > ActorID) then
                        mailbox.Context.Parent <! RouteFinish(RequestFromID, RequestToID, Hops+1)
                    elif (RoutingTable.[samePre].[int (ConvertToBase4String(RequestToID,b).ToCharArray().[samePre].ToString())] <> -1) then
                        mailbox.Context.System.ActorSelection("/user/master/"+string (RoutingTable.[samePre].[int (ConvertToBase4String(RequestToID,b).ToCharArray().[samePre].ToString())])) <! Route(msg, RequestFromID, RequestToID, Hops+1)
                    elif (RequestToID>ActorID) then
                        mailbox.Context.System.ActorSelection("/user/master/"+string (Array.max (LargerLeafSet))) <! Route(msg,RequestFromID,RequestToID,Hops+1)
                        mailbox.Context.Parent <! RouteNotInBoth
                    elif (RequestToID<ActorID) then
                        mailbox.Context.System.ActorSelection("/user/master/"+string (Array.min (SmallerLeafSet))) <! Route(msg,RequestFromID,RequestToID,Hops+1)
                        mailbox.Context.Parent <! RouteNotInBoth
                    else
                        printfn "Impossible"
        | UpdateRow(RowNumber, NewRow) ->
            for i in 0..3 do
                if RoutingTable.[RowNumber].[i] = -1 then
                    RoutingTable.[RowNumber].[i] = NewRow.[i]
        | AddLeaf(Leaf) ->
            addBuffer(Leaf)
            for i in SmallerLeafSet do
                numOfBack <- numOfBack + 1
                mailbox.Context.System.ActorSelection("/user/master/"+(string i)) <! UpdateNodeInfo(ActorID)
            for i in LargerLeafSet do
                numOfBack <- numOfBack + 1
                mailbox.Context.System.ActorSelection("/user/master/"+(string i)) <! UpdateNodeInfo(ActorID)
            for i in 0..b-1 do
                for j in 0..3 do
                    if RoutingTable.[i].[j] <> -1 then
                        numOfBack <- numOfBack + 1
                        mailbox.Context.System.ActorSelection("/user/master/"+(string RoutingTable.[i].[j])) <! UpdateNodeInfo(ActorID)
            for i in 0..b-1 do
                RoutingTable.[i].[int (ConvertToBase4String(ActorID,b).ToCharArray().[i].ToString())] <- ActorID
        | UpdateNodeInfo(newNodeID) ->
            addOne(newNodeID)
        | BeginRouting ->
            for i in 1..NumRequests do
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds 1000., mailbox.Self, Route("Route",ActorID,rand.Next(0,NodeIDSpace),-1))
        | _ ->
            printfn "Unidentified message!!"
        return! receive()
    }
    receive()
    

let Master (mailbox : Actor<_>) =
    let b = ceil(Math.Log(NumNodes|>double)/Math.Log(4.0)) |> int
    let nodeIDSpace = Math.Pow(4.0, b|>double) |> int
    let mutable RandomList = [||]
    let mutable firstGroup = [||]
    let numFirstGroup = NumNodes
    //let mutable i = -1
    let mutable NodesJoined = 0
    let mutable NodesNotInLeafSet = 0
    let mutable TotalRoutes = 0
    let mutable TotalHops = 0
    let mutable NumRoutesNotInBoth = 0
    
    let Init() =
        for i in 0..nodeIDSpace-1 do
            RandomList <- Array.append RandomList [|i|]
        let RandomShuffle a =
            let swap (a: _[]) x y =
                let tmp = a.[x]
                a.[x] <- a.[y]
                a.[y] <- tmp
            Array.iteri (fun i _ -> swap a i (rand.Next(i, Array.length a))) a
        RandomShuffle RandomList
        for i in 0..numFirstGroup-1 do
            firstGroup <- Array.append firstGroup [|RandomList.[i]|]
        for i in 0..NumNodes do
            spawn mailbox.Context (string (RandomList.[i])) (Child(RandomList.[i],b) )
    Init()
       
    let rec receive() = actor {
        let! msg = mailbox.Receive()
        match msg with
        | Go ->
            printfn "Join Begins"
            for i in 0..numFirstGroup do
                mailbox.Context.System.ActorSelection("/user/master/"+(string RandomList.[i])) <! Join(Array.copy firstGroup)
        | JoinFinish ->
            NodesJoined <- NodesJoined + 1
            if NodesJoined = numFirstGroup then
                if NodesJoined >= NumNodes then
                    mailbox.Self <! BeginRouting
                else
                    mailbox.Self <! SecondJoin
            if NodesJoined > numFirstGroup then
                if NodesJoined = NumNodes then
                    mailbox.Self <! BeginRouting
                else
                    mailbox.Self <! SecondJoin
        | SecondJoin ->
            let startID = RandomList.[rand.Next(0, NodesJoined)]
            mailbox.Context.System.ActorSelection("user/master/"+(string startID)) <! Route("Join", startID, RandomList.[NodesJoined], -1)
        | BeginRouting ->
            printfn "Routing Begins"
            mailbox.Context.ActorSelection("/user/master/*") <! BeginRouting
        | NotInBoth ->
            NodesNotInLeafSet <- NodesNotInLeafSet + 1
        | RouteFinish(fromID, toID, hops) ->
            TotalRoutes <- TotalRoutes + 1
            TotalHops <- TotalHops + hops
            for i in 1..10 do
                if (TotalRoutes = NumNodes * NumRequests * i / 10) then
                    printfn  "%i0%% Routing Finished" i
            if TotalRoutes >= NumNodes * NumRequests && not GlobalStop then
                GlobalStop <- true
                printfn "Number of total routes: %i" TotalRoutes
                printfn "Number of total hops: %i" TotalHops
                printfn "Average hops per route: %f" ((TotalHops|>double)/(TotalRoutes |> double))
                mailbox.Context.System.Terminate()
        | RouteNotInBoth ->
            NumRoutesNotInBoth <- NumRoutesNotInBoth + 1
        | _ ->
            printfn "Cannot process the message in master %s" (string msg)
        return! receive()
    }
    receive ()

let masterRef = spawn system "master" Master

masterRef <! Go
system.WhenTerminated.Wait ()